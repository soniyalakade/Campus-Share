import { createDepartmentService } from '../baseService.js';

const service = createDepartmentService({
  department: 'IT',
  httpPort: Number(process.env.IT_PORT || 5002),
  grpcPort: Number(process.env.IT_GRPC_PORT || 6002),
  gatewayPort: Number(process.env.GATEWAY_PORT || 4000)
});

service.start().catch((error) => {
  console.error(`IT startup failed: ${error.message}`);
  process.exitCode = 1;
});

process.once('SIGINT', () => service.stop().then(() => process.exit(0)));
process.once('SIGTERM', () => service.stop().then(() => process.exit(0)));
