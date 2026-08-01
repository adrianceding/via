export const sessionStates = { 1: 'status.session.connecting', 2: 'status.session.authenticating', 3: 'status.session.ready', 4: 'status.session.backoff', 5: 'status.session.closed' };
export const roles = { 1: 'status.role.client', 2: 'status.role.server' };
export const deliveryModes = { 1: 'status.delivery.redundant', 2: 'status.delivery.adaptive' };
export const pathSelections = { 0: '', 1: 'status.path.fastest', 2: 'status.path.distributed' };
export const interfaceReasons = { 1: 'status.interface.available', 2: 'status.interface.excluded', 3: 'status.interface.notIncluded', 4: 'status.interface.disabled', 5: 'status.interface.noAddress', 6: 'status.interface.addressUnavailable' };
export const flowStates = {
  1: 'status.flow.opening', 2: 'status.flow.waiting', 3: 'status.flow.forwarding', 4: 'status.flow.recovering', 5: 'status.flow.closing', 6: 'status.flow.closed', 7: 'status.flow.resetting', 8: 'status.flow.reset',
};
export const adaptiveStates = { 1: 'status.adaptive.single', 2: 'status.adaptive.targetedRetry', 3: 'status.adaptive.fullRedundancy', 4: 'status.adaptive.waiting' };
export const adaptiveTransitions = { 0: '', 1: 'status.adaptiveTransition.ackGap', 2: 'status.adaptiveTransition.retryEscalation', 3: 'status.adaptiveTransition.stableAck', 4: 'status.adaptiveTransition.attachmentsLost', 5: 'status.adaptiveTransition.attachmentRecovered' };
export const transitionReasons = {
  0: '', 1: 'status.reason.started', 2: 'status.reason.pathJoined', 3: 'status.reason.pathRemoved', 4: 'status.reason.authenticationFailed', 5: 'status.reason.protocolConflict',
  6: 'status.reason.resourceLimit', 7: 'status.reason.localIO', 8: 'status.reason.deadline', 9: 'status.reason.cancelled', 10: 'status.reason.remoteReset',
  11: 'status.reason.completedNormally', 12: 'status.reason.internalError',
};