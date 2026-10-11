import crypto from 'node:crypto';

export function createId(prefix = 'id') {
  return `${prefix}_${crypto.randomBytes(12).toString('hex')}`;
}

export function nowISO() {
  return new Date().toISOString();
}

export function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

export function createResource({ department, name, category='General', description='', laboratory='Lab 1', totalQuantity=1, availableQuantity=1, reservedQuantity=0, borrowedQuantity=0, condition='Good', imageUrl='', manualUrl='', tutorialVideoUrl='' }) {
  const total = Number(totalQuantity);
  const available = Number(availableQuantity);
  const reserved = Number(reservedQuantity);
  const borrowed = Number(borrowedQuantity);
  return {
    resourceId: createId('resource'),
    name,
    category,
    description,
    department,
    laboratory,
    totalQuantity: total,
    availableQuantity: available,
    reservedQuantity: reserved,
    borrowedQuantity: borrowed,
    condition,
    imageUrl,
    manualUrl,
    tutorialVideoUrl,
    createdAt: nowISO(),
    updatedAt: nowISO(),
    version: 1,
    vectorClock: { CS: 0, IT: 0, ENTC: 0 }
  };
}

export function seedResources(department, count = 4) {
  const resources = [];
  const templates = [
    { name: 'Arduino Uno', category: 'Development Board', laboratory: 'Embedded Lab', totalQuantity: 4 },
    { name: 'ESP32', category: 'Microcontroller', laboratory: 'IoT Lab', totalQuantity: 3 },
    { name: 'Digital Oscilloscope', category: 'Measurement', laboratory: 'Electronics Lab', totalQuantity: 2 },
    { name: 'Scientific Calculator', category: 'Academic Tool', laboratory: 'Study Hub', totalQuantity: 6 },
    { name: 'Breadboard Kit', category: 'Prototype', laboratory: 'Hardware Lab', totalQuantity: 8 }
  ];
  for (let i = 0; i < count; i += 1) {
    const template = templates[(i + department.length) % templates.length];
    resources.push({
      ...createResource({
        department,
        name: `${template.name}${i > 0 && i % 2 === 0 ? ` ${i}` : ''}`,
        category: template.category,
        description: `${department} resource for academic lab use`,
        laboratory: template.laboratory,
        totalQuantity: template.totalQuantity,
        availableQuantity: template.totalQuantity,
        reservedQuantity: 0,
        borrowedQuantity: 0,
        condition: 'Good',
        imageUrl: '',
        manualUrl: '',
        tutorialVideoUrl: ''
      })
    });
    resources[resources.length - 1].resourceId = `${department.toLowerCase()}_seed_${i + 1}`;
  }
  return resources;
}
