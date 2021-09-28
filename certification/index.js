import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: 'certificate',
  brokers: ['localhost:9092']
})

const topic = 'issue-certificate'
const consumer = kafka.consumer({ groupId: 'certificate-group'})

const producer = kafka.producer()
let counter = 0

async function run() {
  await consumer.connect()
  await consumer.subscribe({topic})

  await consumer.run({
    eachMessage: async ({ topic, partition, message}) => {
      counter++
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
      
      setTimeout(() => {
        producer.send({
          topic: 'certification-response',
          messages: [
            { value: `Certificado #${counter} gerado!`}
          ]
        })
      }, 3000);

    }
  })
}

run().catch(console.error)