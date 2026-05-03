const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'rw-client',
  brokers: ['localhost:9092'],
})

const main = async () => {
  await consume()
  await produce()
  await sleep(30*1000)
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

const consume = async () => {
  const consumer = kafka.consumer({ groupId: 'test-group' })
  await consumer.connect()
  await consumer.subscribe({ topic: 'animal', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      })
    },
  })
}

const produce = async () => {
  const producer = kafka.producer()
  await producer.connect()
  await producer.send({
    topic: 'animal',
    messages: [
      { value: 'goose' },
    ],
  })
  console.log("[Producer]: done sending")
  await producer.disconnect()
}

main()

