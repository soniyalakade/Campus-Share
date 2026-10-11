export const DEPARTMENTS = ['CS', 'IT', 'ENTC'];
export const ROLES = ['STUDENT', 'DEPARTMENT_ADMIN'];
export const ROLE_DEPARTMENTS = {
  STUDENT: null,
  DEPARTMENT_ADMIN: 'CS'
};
export const RESOURCE_STATES = {
  PENDING: 'PENDING',
  APPROVED: 'APPROVED',
  REJECTED: 'REJECTED',
  CANCELLED: 'CANCELLED',
  FULFILLED: 'FULFILLED'
};

export const DEPARTMENT_PORTS = {
  CS: Number(process.env.CS_GRPC_PORT || 6001),
  IT: Number(process.env.IT_GRPC_PORT || 6002),
  ENTC: Number(process.env.ENTC_GRPC_PORT || 6003)
};
