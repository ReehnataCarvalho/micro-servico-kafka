import express from 'express'
import { Kafka } from 'kafkajs'

import routes from './routes'

const PORT = process.env.PORT || 9708
const app = express()

/**
 * Faz a conexão com o Kafka
 */
const kafka = new Kafka({
  clientId: 'api', //Identificador
  brokers: ['localhost:9092'],
  retry: {
    initialRetryTime: 150,
    retries: 10
  },
})

const producer = kafka.producer()
const consumer = kafka.consumer({
  groupId: 'certificate-group-producer'
})

/**
 * Middleware
 * Disponibiliza o producer para todas rotas
*/
app.use((req, res, next) => {
  req.producer = producer;

  return next();
})

/**
 * Cadastra as rotas da aplicação
 */
app.use(routes)

async function run() {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topic: 'certification-response' })

  await consumer.run({
    eachMessage: async ({ topic, partition, message}) => {
      console.log('Resposta', message)
    }
  })

  app.listen(PORT, () => {
    console.info('Servidor rodando na Porta', PORT)
  })
}

run().catch(console.error)


