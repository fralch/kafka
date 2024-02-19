const express = require('express');
const cors = require('cors');
const app = express();
const bodyParser = require('body-parser');

const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'fraud-check-service',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'fraud-check-group' });

// Configuración de Express
app.use(cors()); // cors se utiliza para que el servidor pueda recibir peticiones de otros servidores si no se utiliza no se podra recibir peticiones de otros servidores
app.use(express.json()); // express.json() se utiliza para que el servidor pueda recibir peticiones en formato json

const port = 3000;


// Simulación de una base de datos para almacenar transacciones
const transactionsDatabase = [];

// Recurso para crear una transacción (simulado)
app.post('/create-transaction', async (req, res) => {
  const { accountExternalIdDebit, accountExternalIdCredit, transferTypeId, value } = req.body;

  try {
    // Evaluar si la transacción es fraudulenta (value mayor a 1000)
    const isFraudulent = value > 1000;
    const transactionStatus = isFraudulent ? 'rejected' : 'approved';

    // Simulación de la lógica de creación de transacción
    const transaction = {
      transactionExternalId: generateGuid(),
      accountExternalIdDebit,
      accountExternalIdCredit,
      transferTypeId,
      value,
      status: transactionStatus,
      createdAt: new Date(),
    };

    // Almacenar la transacción simulada en la base de datos simulada
    transactionsDatabase.push(transaction);

    // Enviar los datos de la transacción a Kafka
    await producer.connect();
    await producer.send({
      topic: 'transactions',
      messages: [
        {
          value: JSON.stringify(transaction),
        },
      ],
    });
    await producer.disconnect();

    res.status(201).json({ message: 'Transacción creada exitosamente', status: transactionStatus });
  } catch (error) {
    console.error(`Error al crear la transacción: ${error.message}`);
    res.status(500).json({ error: 'Error al crear la transacción' });
  }
});

// obtener todos las transacciones
app.get('/get-transactions', async (req, res) => {
  try {
    // Simulación de la lógica para obtener todas las transacciones
    const transactions = transactionsDatabase.map((t) => {
      return {
        transactionExternalId: t.transactionExternalId,
        transactionType: { name: 'Compra en línea' }, // Simulación del tipo de transacción
        transactionStatus: { name: t.status }, // Simulación del estado de transacción
        value: t.value,
        createdAt: t.createdAt,
      };
    });

    res.status(200).json(transactions);
  } catch (error) {
    console.error(`Error al obtener las transacciones: ${error.message}`);
    res.status(500).json({ error: 'Error al obtener las transacciones' });
  }
});


// Recurso para obtener detalles de una transacción (simulado)
app.get('/get-transaction/:transactionExternalId', async (req, res) => {
  const { transactionExternalId } = req.params;

  try {
    // Simulación de la lógica para obtener detalles de la transacción
    const transaction = transactionsDatabase.find(
      (t) => t.transactionExternalId === transactionExternalId
    );

    if (!transaction) {
      return res.status(404).json({ error: 'Transacción no encontrada' });
    }

    // Simulación de detalles de transacción
    const transactionDetails = {
      transactionExternalId: transaction.transactionExternalId,
      transactionType: { name: 'Compra en línea' }, // Simulación del tipo de transacción
      transactionStatus: { name: transaction.status }, // Simulación del estado de transacción
      value: transaction.value,
      createdAt: transaction.createdAt,
    };

    res.status(200).json(transactionDetails);
  } catch (error) {
    console.error(`Error al obtener detalles de la transacción: ${error.message}`);
    res.status(500).json({ error: 'Error al obtener detalles de la transacción' });
  }
});

// Función para generar un GUID (simulado)
function generateGuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0,
      v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'transactions', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      console.log(`Datos de transacción recibidos: ${JSON.stringify(data)}`);
      // Puedes agregar lógica adicional para procesar los datos de la transacción según tus necesidades
    },
  });
};

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
