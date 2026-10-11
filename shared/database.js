import mongoose from 'mongoose';

export async function connectDatabase(uri, serviceName) {
  if (!uri) throw new Error(`${serviceName} database URI is not configured`);
  if (mongoose.connection.readyState === 1) return mongoose.connection;
  try {
    await mongoose.connect(uri, {
      serverSelectionTimeoutMS: Number(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS || 5000),
      connectTimeoutMS: Number(process.env.MONGO_CONNECT_TIMEOUT_MS || 5000),
      autoIndex: true
    });
    console.log(`${serviceName} connected to MongoDB database "${mongoose.connection.name}"`);
    return mongoose.connection;
  } catch (error) {
    console.error(`${serviceName} MongoDB connection failed: ${error.message}`);
    throw error;
  }
}

export function isDatabaseReady() {
  return mongoose.connection.readyState === 1;
}

export async function disconnectDatabase() {
  if (mongoose.connection.readyState !== 0) await mongoose.disconnect();
}
