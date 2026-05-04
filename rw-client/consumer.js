const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'rw-client',
  brokers: ['localhost:9092'],
})

const main = async () => {
  await consume()
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

main()

