import { createDepartmentService } from '../baseService.js';

const service = createDepartmentService({
  department: 'CS',
  httpPort: Number(process.env.CS_PORT || 5001),
  grpcPort: Number(process.env.CS_GRPC_PORT || 6001),
  gatewayPort: Number(process.env.GATEWAY_PORT || 4000)
});

service.start().catch((error) => {
  console.error(`CS startup failed: ${error.message}`);
  process.exitCode = 1;
});

process.once('SIGINT', () => service.stop().then(() => process.exit(0)));
process.once('SIGTERM', () => service.stop().then(() => process.exit(0)));
