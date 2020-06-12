const {Transform} = require('stream')

const pg_tz = _ => new Date(Number(_ / 1000n) + 946684800000)

const extractString = function(_) {
	const index = this.indexOf(0, _)
	if (index === -1)
		throw new Error('Unknown Message Format')
	return this.slice(_, index).toString()
}

const extractTuples = function(_) {
	const cols = this.readUInt16BE(_)
	return Array.from(new Array(cols)).reduce(([r, counter]) => {
		const format = this.toString('utf8', counter, counter + 1)
		if (['n','u'].includes(format)) {
			return [[...r, {format}], counter + 1]
		} else if (format === 't') {
			const length = this.readUInt32BE(counter + 1)
			const value = this.slice(counter + 1 + 4, counter + 1 + 4 + length).toString()
			return [[...r, {format, value}], counter + 1 + 4 + length]
		} else {
			throw new Error('Unknown Message Format')
		}
	}, [[], _ + 2])
}

const tuples = {
	0x4b: 'KEY',
	0x4f: 'OLD',
	0x4e: 'NEW'
}

const createParsers = ({typeParsers, includeTransactionLsn, includeXids, includeTimestamp}) => {
	const relations = new Map()
	const parseColumn = function({format, value}) {
		//
		// XXX: Not sure how to handle toasted value
		//
		return format === 't' ? this(value) : format === 'n' ? null : undefined
	}

	return {
		0x42: _ => { // Begin
			return [Object.assign({}, {kind: 'BEGIN'}, 
				includeTransactionLsn ? {final_lsn: _.readBigUInt64BE(1)} : {},
				includeTimestamp ? {timestamp: pg_tz(_.readBigUInt64BE(1 + 8))} : {},
				includeXids ? {xid : _.readUInt32BE(1 + 8 + 8)} : {}
				), 1 + 8 + 8 + 4]
		},
		0x43: _ => { // Commit
			const flags = _.readUInt8(1)
			return [Object.assign({}, {kind: 'COMMIT', flags},
				includeTransactionLsn ? {lsn: _.readBigUInt64BE(1 + 1), end_lsn: _.readBigUInt64BE(1 + 1 + 8)} : {},
				includeTimestamp ? {timestamp: pg_tz(_.readBigUInt64BE(1 + 1 + 8 + 8))} : {}
				), 1 + 1 + 8 + 8 + 8]
		},
		0x44: _ => { // Delete
			const id = _.readUInt32BE(1)
			const identifier = _.readUInt8(1 + 4)
			if (identifier in tuples && relations.has(id)) {
				const {schema, table, columns} = relations.get(id)
				const [v, counter] = extractTuples.call(_, 1 + 4 + 1)
				const row = Object.fromEntries(Object.entries(columns)
					.filter(([k, {flags}]) => identifier !== 0x4b || flags === 1)
					.map(([k, {typeParser}], i) => [k, parseColumn.call(typeParser, v[i])]))
				return [{kind: 'DELETE', schema, table, [tuples[identifier]]: row}, counter]
			} else
				throw new Error(`Unknown Update Message Format: ${identifier.toString(16)}`)
		},
		0x49: _ => { // Insert
			const id = _.readUInt32BE(1)
			const identifier = _.readUInt8(1 + 4)
			if (identifier in tuples && relations.has(id)) {
				const {schema, table, columns} = relations.get(id)
				const [v, counter] = extractTuples.call(_, 1 + 4 + 1)
				const row = Object.fromEntries(Object.entries(columns)
					.map(([k, {typeParser}], i) => [k, parseColumn.call(typeParser, v[i])]))
				return [{kind: 'INSERT', schema, table, [tuples[identifier]]: row}, counter]
			} else
				throw new Error(`Unknown Insert Message Format: ${identifier.toString(16)}`)
		},
		0x4f: _ => { // Origin
			const lsn = _.readBigUInt64BE(1)
			const name = extractString.call(_, 1 + 8)
			return [{kind: 'ORIGIN', lsn, name}, 1 + 8 + name.length + 1]
		},
		0x52: _ => { // Relation
			const id = _.readUInt32BE(1)
			const schema = extractString.call(_, 1 + 4)
			const table = extractString.call(_, 1 + 4 + schema.length + 1)
			const setting = _.readUInt8(1 + 4 + schema.length + 1 + table.length + 1)
			const cols = _.readUInt16BE(1 + 4 + schema.length + 1 + table.length + 1 + 1)
			const [columns, counter] = Array.from(new Array(cols)).reduce(([r, counter]) => {
				const flags = _.readUInt8(counter)
				const name = extractString.call(_, counter + 1)
				const id = _.readUInt32BE(counter + 1 + name.length + 1)
				const type_modifier = _.readUInt32BE(counter + 1 + name.length + 1 + 4)
				const typeParser = typeParsers ? typeParsers.getTypeParser(id) : _ => _
				return [{...r, [name]: {flags, id, type_modifier, typeParser}}, counter + 1 + name.length + 1 + 4 + 4]
			}, [{}, 1 + 4 + schema.length + 1 + table.length + 1 + 1 + 2])
			const relation = {kind: 'RELATION', id, schema, table, setting, columns}
			relations.set(id, relation)
			return [relation, counter]
		},
		0x54: _ => { // Truncate
			const rels = _.readUInt32BE(1)
			const option = _.readUInt8(1 + 4)
			const [id, c] = Array.from(new Array(rels)).reduce(([r, counter]) => [[...r, _.readUInt32BE(counter)], counter + 4], [[], 1 + 4 + 1])
			return [{kind: 'TRUNCATE', option, relations: id.map(_ => relations.get(_)).map(({schema, table}) => ({schema, table}))}, c]
		},
		0x55: _ => { // Update
			const kind = 'UPDATE'
			const id = _.readUInt32BE(1)
			const identifier = _.readUInt8(1 + 4)
			if (identifier in tuples && relations.has(id)) {
				const {schema, table, columns} = relations.get(id)
				const [v, counter] = extractTuples.call(_, 1 + 4 + 1)
				const row = Object.fromEntries(Object.entries(columns)
					.filter(([k, {flags}]) => identifier !== 0x4b || flags === 1)
					.map(([k, {typeParser}], i) => [k, parseColumn.call(typeParser, v[i])]))
				if (identifier !== 0x4e) {
					const nn = _.readUInt8(counter)
					if (nn !== 0x4e)
						throw new Error(`Unknown Update Message Format: ${nn.toString(16)}`)
					const [n, c] = extractTuples.call(_, counter + 1)
					const nrow = Object.fromEntries(Object.entries(columns)
						.map(([k, {typeParser}], i) => [k, parseColumn.call(typeParser, n[i])]))
					return [{kind, schema, table, [tuples[identifier]]: row, [tuples[nn]]: nrow}, c]
				}
				else
					return [{kind, schema, table, [tuples[identifier]]: row}, counter]
			} else
				throw new Error(`Unknown Update Message Format: ${identifier.toString(16)}`)
		},
		0x59: () => { // Type
			const id = _.readUInt32BE(1)
			const schema = extractString.call(_, 1 + 4)
			const name = extractString.call(_, 1 + 4 + schema.length + 1)
			return [{kind: 'TYPE', id, schema, name}, 1 + 4 + schema.length + 1 + name.length + 1]
		}
	}
}

class PgOutputParser extends Transform {
	constructor(options) {
		super({objectMode: true})
		this.options = options || {}
		this.parsers = this.options.parsers || createParsers(this.options)
	}

	_transform(data, encoding, callback) {
		let counter = 0
		const {includeLsn, includeTimestamp} = this.options
		while (true) {
			const chunk = data.slice(counter)
			if (chunk.length === 0) break
			if (chunk[0] === 0x77) {
				const wal_data = chunk.slice(1 + 8 + 8 + 8)

				const [header] = wal_data
				if (header in this.parsers) {
					const [result, consumed] = this.parsers[header](wal_data)
					this.push(Object.assign({},
						includeLsn ? {lsn: chunk.readBigUInt64BE(1), end: chunk.readBigUInt64BE(1 + 8)} : {},
						includeTimestamp ? {timestamp: chunk.readBigUInt64BE(1 + 8 + 8)} : {},
						result))
					counter += 1 + 8 + 8 + 8 + consumed
				} else {
					callback(new Error(`Unknown Header: ${header.toString(16)}`))
					return
				}
			} else {
				callback(new Error(`Unknown Header: ${chunk[0].toString(16)}`))
				return
			}
		}
		process.nextTick(callback)
	}
}

module.exports = PgOutputParser
