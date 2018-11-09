'use strict'

const amqp = require(`amqplib`)
const randomString = require(`randomstring`)
const log4js = require(`log4js`)

log4js.configure({
	appenders: {console: {type: `console`}},
	categories: {default: {appenders: [`console`], level: `trace`}}
})

const DEFAULT_RABBITMQ_DATA = {
	requiredQueues: [],
	queue: {
		connectionOptions: {
			protocol: `amqp`,
			user: `guest`,
			password: `guest`,
			host: `127.0.0.1`,
			port: 5672,
		},
		assertOptions: {},
	},
}

const isArray = (paramName, source) =>
	paramName in source && Array.isArray(source[paramName])
const isFunction = (paramName, source) =>
	paramName in source && typeof source[paramName] === `function`
const checkLogger = source =>
	source && [`info`, `error`, `trace`, `debug`].every(item => isFunction(item, source))
		? source : null

module.exports = class Helper {
	constructor(logger) {
		this.queueCheckFlag = false
		this.logger = checkLogger(logger) || log4js.getLogger()

		this.RabbitMQ = {
			checkQueue: {
				interval: 300000,
				repeat: true,
			},
		}

		this.setRMQData = this.setRMQData.bind(this)
		this.setRMQDefaults = this.setRMQDefaults.bind(this)
		this.assertRMQ = this.assertRMQ.bind(this)
		this.createRMQChannel = this.createRMQChannel.bind(this)
		this.createRequiredQueues = this.createRequiredQueues.bind(this)
		this.addRequiredListeners = this.addRequiredListeners.bind(this)
		this.consumeRMQMessage = this.consumeRMQMessage.bind(this)
		this.prepareQueue = this.prepareQueue.bind(this)
		this.channelErrorListener = this.channelErrorListener.bind(this)
		this.getMQChannel = this.getMQChannel.bind(this)
		this.checkQueue = this.checkQueue.bind(this)
		this.setCheckQueueInterval = this.setCheckQueueInterval.bind(this)
		this.stopCheckQueueInterval = this.stopCheckQueueInterval.bind(this)
		this.sendRMQMessage = this.sendRMQMessage.bind(this)
		this.disconnectRMQ = this.disconnectRMQ.bind(this)
		this.sendRMQack = this.sendRMQack.bind(this)

		this.setRMQDefaults()
	}
	setRMQData(data) {
		this.logger.info(`setRMQData`)
		this.logger.trace(JSON.stringify(data))

		if (isArray(`requiredQueues`, data)) {
			this.RabbitMQ.data.requiredQueues = data.requiredQueues
		}
		if (`queue` in data) {
			if (`connectionOptions` in data.queue) {
				this.RabbitMQ.data.queue.connectionOptions = {
					...this.RabbitMQ.data.queue.connectionOptions,
					...data.queue.connectionOptions,
				}
			}
			if (`assertOptions` in data.queue) {
				this.RabbitMQ.data.queue.assertOptions = data.queue.assertOptions
			}
		}
		if (isArray(`requiredListeners`, data)) {
			this.RabbitMQ.data.requiredListeners = data.requiredListeners
		}
		if (isFunction(`customChannelErrorListener`, data)) {
			this.RabbitMQ.data.customChannelErrorListener = data.customChannelErrorListener
		}
		if (isFunction(`customChannelReadyListener`, data)) {
			this.RabbitMQ.data.customChannelReadyListener = data.customChannelReadyListener
		}

		return this.prepareQueue()
	}
	// set default RabbitMQ data
	setRMQDefaults() {
		this.logger.info(`Set RMQ default data`)
		this.RabbitMQ.data = {...DEFAULT_RABBITMQ_DATA}
	}
	createRMQChannel() {
		this.logger.info(`createRMQChannel`)

		const options = this.RabbitMQ.data.queue.connectionOptions

		this.logger.trace(options)
		this.logger.info(`Preparing connection URL`)

		let url = ``
		url += `protocol` in options
			? options.protocol : DEFAULT_RABBITMQ_DATA.queue.connectionOptions.protocol
		url += `://`
		url += `user` in options ? options.user : DEFAULT_RABBITMQ_DATA.queue.connectionOptions.user
		url += `:`
		url += `password` in options
			? options.password : DEFAULT_RABBITMQ_DATA.queue.connectionOptions.password
		url += `@`
		url += `host` in options ? options.host : DEFAULT_RABBITMQ_DATA.queue.connectionOptions.host
		url += `:`
		url += `port` in options ? options.port : DEFAULT_RABBITMQ_DATA.queue.connectionOptions.port
		this.logger.debug(url)

		return new Promise(async (resolve, reject) => {
			try {
				this.logger.info(`Connecting to RabbitMQ host`)
				const connection = await amqp.connect(url)

				this.logger.info(`Creating connection channel`)
				const channel = await connection.createConfirmChannel()

				if (connection && channel) {
					this.RabbitMQ.conn = connection
					this.RabbitMQ.ch = channel

					resolve()
				}
				else {
					reject(`Connection && Channel undefined`)
				}
			}
			catch (err) {
				reject(err)
			}
		})
	}
	assertRMQ(qname) {
		this.logger.info(`Asserting RabbitMQ queue ${qname}`)
		// When RabbitMQ quits or crashes it will forget the queues and messages
		// unless you tell it not to. 
		// Two things are required to make sure that messages aren't lost: 
		// we need to mark both the queue and messages as durable.
		return this.RabbitMQ.ch.assertQueue(qname, this.RabbitMQ.data.queue.assertOptions)
	}
	createRequiredQueues() {
		return this.RabbitMQ.data.requiredQueues && this.RabbitMQ.data.requiredQueues.map(queue => {
			this.logger.info(`createRequiredQueue - ${queue}`)
			return this.assertRMQ(queue)
		})
	}
	addRequiredListeners() {
		return this.RabbitMQ.data.requiredListeners
			&& this.RabbitMQ.data.requiredListeners.map(listener => {
				if (`name` in listener && `options` in listener && `worker` in listener) {
					this.logger.info(`Add RMQ Listener: ${listener.name}`)
					return this.consumeRMQMessage(listener)
				}

				this.logger.info(`No required fields in listener: ${listener.name}`)
				return null

			})
	}
	consumeRMQMessage({name, options, worker}) {
		return this.RabbitMQ.ch.consume(name, msg => {
			this.logger.info(`Received RabbitMQ message:`)
			if (`correlationId` in msg.properties) {
				this.logger.info(`Message Id: ${msg.properties.correlationId}`)
			}
			this.logger.debug(JSON.stringify(msg.content))
			this.logger.trace(msg.content.toString())
			this.logger.debug(JSON.stringify(msg.properties))
			worker(msg)
		}, options)
	}
	prepareQueue() {
		if (!`data` in this.RabbitMQ) {
			this.setRMQDefaults()
		}

		const data = this.RabbitMQ.data

		this.logger.info(`Preparing RabbitMQ...`)

		return new Promise(async (resolve, reject) => {
			try {
				await this.createRMQChannel()

				this.logger.info(`createQueues`)
				await Promise.all(this.createRequiredQueues())

				this.logger.info(`configureReportListener`)
				// The prefetch method with the value of 1 tells RabbitMQ not to give more than
				// one message to a worker at a time. 
				// Or, in other words, don't dispatch a new message to a worker until it has processed
				// and acknowledged the previous one.
				// Instead, it will dispatch it to the next worker that is not still busy.
				this.RabbitMQ.ch.prefetch(1)

				// Add channel error listener
				this.RabbitMQ.ch.on(`error`, this.channelErrorListener)

				// Add required listeners
				this.logger.info(`Check required listeners`)
				if (`requiredListeners` in data) {
					await Promise.all(this.addRequiredListeners())
				}
				else {
					this.logger.info(`No listeners required`)
				}

				this.logger.info(`Preparing RabbitMQ Complete !`)
				if (`customChannelReadyListener` in data) {
					data.customChannelReadyListener()
				}
				resolve()
			}
			catch (err) {
				this.logger.error(`Error prepare RabbitMQ`)
				this.channelErrorListener(err)
				reject(err)
			}
		})
	}
	// check RabbitMQ channel, запуск нового соединения при необходимости
	getMQChannel() {
		return new Promise(resolve => {
			let reconnect = true

			this.logger.info(`getMQChannel`)
			if (`ch` in this.RabbitMQ && this.RabbitMQ.ch) {
				reconnect = false
			};

			if (!reconnect) {
				this.logger.info(`RabbitMQ channel OK`)
				return resolve(this.RabbitMQ.ch)
			}

			this.logger.info(`RabbitMQ channel need to reconnect`)

			this.prepareQueue()
				.then(() => resolve(this.RabbitMQ.ch))
				.catch(() => resolve(null))
		})
	}
	// run check RAbbitMQ channel with interval
	checkQueue() {
		if (this.queueCheckFlag) return

		this.queueCheckFlag = true
		this.logger.info(`checkQueue planned - interval ${this.RabbitMQ.checkQueue.interval}`)

		setTimeout(() => {
			this.logger.info(`checkQueue`)
			this.getMQChannel()

			if (this.RabbitMQ.checkQueue.repeat) {
				this.queueCheckFlag = false
				this.checkQueue()
			}
		}, this.RabbitMQ.checkQueue.interval)
	}
	setCheckQueueInterval(interval, stop) {
		if (interval) {
			this.logger.info(`Create checkQueue planned - interval ${interval}`)
			this.RabbitMQ.checkQueue.interval = interval
		}
		else {
			this.logger.info(`Create checkQueue default planned - interval `,
				this.RabbitMQ.checkQueue.interval)
		}

		if (!stop) {
			this.RabbitMQ.checkQueue.repeat = true
			this.checkQueue()
		}
	}
	stopCheckQueueInterval() {
		this.logger.info(`Stop checkQueue planned task`)
		this.RabbitMQ.checkQueue.repeat = false
		this.logger.trace(`Check interval task flag - ${this.RabbitMQ.checkQueue.repeat}`)
	}
	sendRMQMessage(qname, msg, options) {
		return new Promise(async (resolve, reject) => {
			const channel = await this.getMQChannel()
			const _options = {...options, correlationId: randomString.generate(32)}

			this.logger.info(`Sending RabbitMQ message`)
			this.logger.trace(JSON.stringify(msg))

			if (!channel) {
				const error = `Can't send message, channel not ready`
				this.logger.error(error)
				return reject(error)
			}

			// At this point we're sure that the task_queue queue won't be lost
			// even if RabbitMQ restarts. 
			// Now we need to mark our messages as persistent - by using
			// the persistent option Channel.sendToQueue takes.
			// Note: on Node 6 Buffer.from(msg) should be used
			try {
				await channel.sendToQueue(qname, Buffer.from(JSON.stringify(msg)), _options)

				this.logger.info(`RabbitMQ message sent. ID: ${_options.correlationId}`)
				return resolve(_options.correlationId)
			}
			catch (err) {
				this.logger.error(err)
				return reject(err)
			}
		})
	}
	sendRMQack(msg) {
		return new Promise(async (resolve, reject) => {
			this.logger.info(`sendRMQack`)
			// If a #consume or #get is issued with noAck: false (the default), 
			// the server will expect acknowledgements for messages before forgetting about them. 
			// If no such acknowledgement is given, those messages may be requeued
			// once the channel is closed.

			const channel = await this.getMQChannel()
			if (!channel) {
				const error = `Can't send acknowledgement, channel not ready`
				this.logger.error(error)
				return reject(error)
			}

			this.logger.info(`Send acknowledgement`)
			channel.ack(msg)

			this.logger.info(`Complete`)
			return resolve()
		})
	}
	disconnectRMQ() {
		this.logger.info(`Disconnecting from RabbitMQ`)
		const {conn} = this.RabbitMQ

		this.RabbitMQ.conn = null
		this.RabbitMQ.ch = null

		return conn.close()
	}
	// optional error listeners
	channelErrorListener(err) {
		this.logger.error(`RabbitMQ Cnannel ERROR`, err)

		if (`code` in err && (err.code === `EHOSTUNREACH` || err.code === `ETIMEDOUT`)) {
			this.RabbitMQ.ch = null
		}

		if (this.RabbitMQ.data.customChannelErrorListener) {
			this.logger.info(`Call custom error listener`)
			this.RabbitMQ.data.customChannelErrorListener(err)
		}
	}
}