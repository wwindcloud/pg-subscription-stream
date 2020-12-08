const {Transform} = require('stream')
const {both} = require('pg-copy-streams')

const now = () => BigInt(Date.now()) - 946684800000n
const invalid_lsn = 0n

class PgSubscriptionStream extends Transform {
	constructor(options) {
		super()
		this.options = options || {}
		this.output_written_lsn = invalid_lsn
		this.flush_written_lsn = invalid_lsn
		this.last_feedback_time = now()

		const {slotName, feedbackInterval = 20000, startPos = 0n, pluginOptions = {
			proto_version: 1,
			publication_names: slotName
		}} = this.options
		const lsn = typeof(startPos) === 'bigint' ? startPos.toString(16).padStart(9, '0').replace(/.{8}$/, '\/$&') : startPos
		const query = `START_REPLICATION SLOT ${slotName} LOGICAL ${lsn} (${Object.entries(pluginOptions).map(([k, v]) => `"${k}" '${v}'`).join(',')})`
		this.copyBoth = new both(query, {
			alignOnCopyDataFrame: true
		})
		this.copyBoth.pipe(this)
		this.copyBoth.on('error', err => this.emit('error', err))
		this.interval = setInterval(() => {
			this.sendFeedback()
		}, feedbackInterval)
		this.on('end', () => {
			clearInterval(this.interval)
			this.copyBoth.end()
		})
	}

	sendFeedback(force) {
		if (this.flush_written_lsn === invalid_lsn) return

		const current_time = now()
		const {feedbackInterval = 20000} = this.options
		if (force || current_time - this.last_feedback_time > feedbackInterval) {
			this.last_feedback_time = current_time
			const response = new DataView(new ArrayBuffer(1 + 8 + 8 + 8 + 8 + 1))
			response.setUint8(0, 'r'.charCodeAt(0))
			response.setBigUint64(1, this.output_written_lsn)
			response.setBigUint64(1 + 8, this.flush_written_lsn)
			response.setBigUint64(1 + 8 + 8, invalid_lsn)
			response.setBigUint64(1 + 8 + 8 + 8, current_time)
			response.setUint8(1 + 8 + 8 + 8 + 8, 0)
			this.copyBoth.write(Buffer.from(response.buffer))
		}
	}

	_transform(chunk, encoding, callback) {
		const {autoConfirmLSN = true} = this.options
		const [header] = chunk
		if (header === 0x77) {
			const lsn = chunk.readBigUInt64BE(1)
			this.push(chunk)
			this.output_written_lsn = this.output_written_lsn > lsn ? this.output_written_lsn : lsn
			this.flush_written_lsn = autoConfirmLSN ? this.output_written_lsn : this.flush_written_lsn
		} else if (header === 0x6b) {
			const lsn = chunk.readBigUInt64BE(1)
			const shouldRespond = chunk.readInt8(1 + 8 + 8)
			this.output_written_lsn = this.output_written_lsn > lsn ? this.output_written_lsn : lsn
			this.flush_written_lsn = autoConfirmLSN ? this.output_written_lsn : this.flush_written_lsn
			this.sendFeedback(shouldRespond > 0)
		} else {
			callback(new Error(`Unknown Message: ${chunk}`))
			return
		}
		process.nextTick(callback)
	}

	submit(connection) {
		this.copyBoth.submit(connection)
	}

	confirmLSN(lsn) {
		this.flush_written_lsn = lsn > this.flush_written_lsn ? lsn : this.flush_written_lsn
	}

	handleError(e) {
		this.copyBoth.handleError(e)
	}

	handleCopyData(chunk) {
		this.copyBoth.handleCopyData(chunk)
	}

	handleCommandComplete() {
		this.copyBoth.handleCommandComplete()
	}

	handleReadyForQuery() {
		this.copyBoth.handleReadyForQuery()
	}
}

module.exports = PgSubscriptionStream
