export function fastestEndpointValues(session) {
  return {
    local: session?.local_endpoint || session?.local_address || '--',
    remote: session?.remote_endpoint || '--',
  };
}
