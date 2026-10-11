import mongoose from 'mongoose';

const eventReceiptSchema = new mongoose.Schema({
  eventId: { type: String, required: true, unique: true },
  relayStatus: { type: String, required: true, enum: ['pending', 'relayed', 'accepted_no_clients'] },
  event: { type: mongoose.Schema.Types.Mixed, required: true },
  relayLeaseUntil: { type: Date, default: null },
  expireAt: { type: Date, default: null },
  updatedAt: { type: Date, default: Date.now }
}, { versionKey: false });

eventReceiptSchema.index({ expireAt: 1 }, { expireAfterSeconds: 0 });

export default mongoose.models.EventReceipt || mongoose.model('EventReceipt', eventReceiptSchema);
