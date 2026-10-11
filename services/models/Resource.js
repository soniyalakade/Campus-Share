import mongoose from 'mongoose';

const reservationSchema = new mongoose.Schema({
  reservationId: { type: String, required: true },
  resourceId: { type: String, required: true },
  department: { type: String, required: true, enum: ['CS', 'IT', 'ENTC'] },
  studentId: { type: String, required: true },
  status: { type: String, required: true, enum: ['PENDING', 'APPROVED', 'REJECTED', 'CANCELLED', 'FULFILLED'] },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
}, { _id: false });

const borrowRecordSchema = new mongoose.Schema({
  borrowRecordId: { type: String, required: true },
  resourceId: { type: String, required: true },
  department: { type: String, required: true, enum: ['CS', 'IT', 'ENTC'] },
  reservationId: { type: String, required: true },
  studentId: { type: String, required: true },
  borrowedAt: { type: Date, default: Date.now },
  returnedAt: { type: Date, default: null },
  status: { type: String, required: true, enum: ['BORROWED', 'RETURNED'] }
}, { _id: false });

const outboxEntrySchema = new mongoose.Schema({
  eventId: { type: String, required: true },
  eventType: { type: String, required: true },
  sourceNode: { type: String, required: true },
  sourceDepartment: { type: String, required: true, enum: ['CS', 'IT', 'ENTC'] },
  timestamp: { type: Date, required: true },
  lamportTimestamp: { type: Number, min: 0 },
  resourceId: { type: String, required: true },
  reservationId: { type: String, default: null },
  borrowRecordId: { type: String, default: null },
  payload: { type: mongoose.Schema.Types.Mixed, default: () => ({}) },
  rabbitmqStatus: { type: String, enum: ['pending', 'confirmed'], default: 'pending' },
  websocketRelayStatus: { type: String, enum: ['pending', 'relayed', 'accepted_no_clients'], default: 'pending' },
  attempts: { type: Number, default: 0, min: 0 },
  nextAttemptAt: { type: Date, default: Date.now },
  claimedBy: { type: String, default: null },
  leaseUntil: { type: Date, default: null },
  completedAt: { type: Date, default: null },
  lastErrorCode: { type: String, default: null }
}, { _id: false });

const resourceSchema = new mongoose.Schema({
  resourceId: { type: String, required: true, unique: true, index: true },
  name: { type: String, required: true, trim: true, maxlength: 160 },
  category: { type: String, required: true, trim: true, maxlength: 100 },
  description: { type: String, default: '', maxlength: 4000 },
  department: { type: String, required: true, enum: ['CS', 'IT', 'ENTC'], index: true },
  laboratory: { type: String, default: 'Lab 1', maxlength: 160 },
  totalQuantity: { type: Number, required: true, min: 0 },
  availableQuantity: { type: Number, required: true, min: 0 },
  reservedQuantity: { type: Number, required: true, min: 0 },
  borrowedQuantity: { type: Number, required: true, min: 0 },
  condition: { type: String, default: 'Good', maxlength: 80 },
  imageUrl: { type: String, default: '' },
  manualUrl: { type: String, default: '' },
  tutorialVideoUrl: { type: String, default: '' },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
  version: { type: Number, default: 1, min: 1 },
  isDeleted: { type: Boolean, default: false, index: true },
  vectorClock: { type: mongoose.Schema.Types.Mixed, default: () => ({ CS: 0, IT: 0, ENTC: 0 }) },
  reservations: { type: [reservationSchema], default: [] },
  borrowRecords: { type: [borrowRecordSchema], default: [] },
  outbox: { type: [outboxEntrySchema], default: [] }
}, { versionKey: false });

resourceSchema.index({ department: 1, name: 1 });
resourceSchema.index({ department: 1, 'reservations.studentId': 1, 'reservations.status': 1 });
resourceSchema.index({ department: 1, 'reservations.reservationId': 1 });
resourceSchema.index({ department: 1, 'borrowRecords.studentId': 1, 'borrowRecords.status': 1 });
resourceSchema.index({ department: 1, 'borrowRecords.borrowRecordId': 1 });
resourceSchema.index({ department: 1, 'outbox.eventId': 1 });
resourceSchema.index({ department: 1, 'outbox.rabbitmqStatus': 1, 'outbox.nextAttemptAt': 1, 'outbox.leaseUntil': 1 });

export const ResourceSchema = resourceSchema;
export function createResourceModel(connection) {
  return connection.models.Resource || connection.model('Resource', resourceSchema);
}

export default mongoose.models.Resource || mongoose.model('Resource', resourceSchema);
