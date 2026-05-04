const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'rw-client',
  brokers: ['localhost:9092'],
})

const main = async () => {
  await produce()
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
  await producer.send({
    topic: 'animal',
    messages: [
      { value: 'cat' }, { value: 'dog' }
    ],
  })
  console.log("[Producer]: done sending")
  await producer.disconnect()
}

main()

