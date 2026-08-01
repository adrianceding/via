const REFRESH_INTERVAL_MS = 3000;

export function createPoller(refresh, schedule = setTimeout, cancel = clearTimeout) {
  let stopped = false;
  let paused = false;
  let timer = null;
  let inFlight = null;

  function clearScheduled() {
    if (timer !== null) cancel(timer);
    timer = null;
  }

  function scheduleNext() {
    if (stopped || paused || timer !== null) return;
    timer = schedule(() => {
      timer = null;
      void run();
    }, REFRESH_INTERVAL_MS);
  }

  function run() {
    if (stopped) return Promise.resolve();
    if (inFlight) return inFlight;
    inFlight = (async () => {
      try {
        await refresh();
      } finally {
        inFlight = null;
        scheduleNext();
      }
    })();
    return inFlight;
  }

  return {
    start: run,
    get paused() { return paused; },
    get refreshing() { return inFlight !== null; },
    pause() {
      paused = true;
      clearScheduled();
    },
    refreshNow() {
      clearScheduled();
      return run();
    },
    resume() {
      if (stopped) return Promise.resolve();
      paused = false;
      clearScheduled();
      return run();
    },
    stop() {
      stopped = true;
      clearScheduled();
    },
  };
}