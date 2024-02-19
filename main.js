
const express = require('express');
const cors = require('cors');
const app = express();
const bodyParser = require('body-parser');

const { Kafka } = require('kafkajs');

// Configuración de Kafka
const kafka = new Kafka({
  clientId: 'example-app',
  brokers: ['localhost:9092'], // Asegúrate de cambiar esto según tu configuración de Kafka
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'example-group' });

// Configuración de Express
app.use(cors()); // cors se utiliza para que el servidor pueda recibir peticiones de otros servidores si no se utiliza no se podra recibir peticiones de otros servidores
app.use(express.json()); // express.json() se utiliza para que el servidor pueda recibir peticiones en formato json

const port = 3000;

// Ruta para enviar mensajes a Kafka (Productor)
app.post('/produce', async (req, res) => {
  const { message } = req.body;

  try {
    await producer.connect();
    await producer.send({
      topic: 'example-topic',
      messages: [{ value: message }],
    });
    await producer.disconnect();

    res.status(200).send('Mensaje enviado a Kafka');
  } catch (error) {
    console.error(`Error al enviar el mensaje: ${error.message}`);
    res.status(500).send('Error al enviar el mensaje a Kafka');
  }
});

// Inicializar el consumidor
const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'example-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log(`Mensaje recibido: ${message.value.toString()}`);
      // Aquí puedes agregar la lógica de procesamiento del mensaje
    },
  });
};

// Iniciar la aplicación
const startApp = async () => {
  try {
    await runConsumer();
    app.listen(port, () => {
      console.log(`Servidor Express escuchando en http://localhost:${port}`);
    });
  } catch (error) {
    console.error(`Error al iniciar la aplicación: ${error.message}`);
  }
};

startApp();
