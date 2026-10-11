import { createDepartmentService } from '../baseService.js';

const service = createDepartmentService({
  department: 'ENTC',
  httpPort: Number(process.env.ENTC_PORT || 5003),
  grpcPort: Number(process.env.ENTC_GRPC_PORT || 6003),
  gatewayPort: Number(process.env.GATEWAY_PORT || 4000)
});

service.start().catch((error) => {
  console.error(`ENTC startup failed: ${error.message}`);
  process.exitCode = 1;
});

process.once('SIGINT', () => service.stop().then(() => process.exit(0)));
process.once('SIGTERM', () => service.stop().then(() => process.exit(0)));
