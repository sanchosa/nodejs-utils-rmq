# nodejs-utils-rmq
RabbitMQ helper utility for nodeJS

# example

```javascript
const QueueHelper = require(`nodejs-utils-rmq`)

const queueConfig = {
	QUEUE: {
		CHECK_INTERVAL: 300000,
		CONNECTION_OPTIONS: {
			host: `some-host`
		},
		ASSERT_OPTIONS: {
			durable: true
		}
	},
	MY_QUEUE: {
		CONSUME_OPTIONS: {
			noAck: false
		},
		SEND_OPTIONS: {
			persistent: true,
			content_type: `application/json`
		},
		NAME: `myQueue`,
	},
}

const rmq = new QueueHelper() // new QueueHelper(customLogger like log4js)
const sendMessage = () => {
	rmq.sendRMQMessage(queueConfig.MY_QUEUE.NAME,
		{id: `test id`, data: `test data`},
		queueConfig.MY_QUEUE.SEND_OPTIONS)
		.then(res => console.log(res))
		.catch(err => console.error(err))
}
const processMessage = msg => {
	// do something
	console.log(`Process message...`)

	let msgId = `noId`

	if (`correlationId` in msg.properties) {
		msgId = msg.properties.correlationId
		console.log(`Incoming message: ${msgId}`)
	}

	rmq.sendRMQack(msg)
}
const params = {
	requiredQueues: [queueConfig.EMAIL_QUEUE.NAME, queueConfig.ACTS_QUEUE.NAME],
	queue: {
		connectionOptions: queueConfig.QUEUE.CONNECTION_OPTIONS,
		assertOptions: queueConfig.QUEUE.ASSERT_OPTIONS
	},
	requiredListeners: [{
		name: queueConfig.MY_QUEUE.NAME,
		options: queueConfig.MY_QUEUE.CONSUME_OPTIONS,
		worker: processMessage,
	}],
	customChannelReadyListener: () => sendMessage()
}

rmq.setRMQData(params)
	.then(() => console.log(`Ready !`))
	.catch(err => console.error(err))

rmq.setCheckQueueInterval(queueConfig.QUEUE.CHECK_INTERVAL)

```
