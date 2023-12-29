export const logger = ({ log, debug }: { log?: boolean; debug?: boolean }) => ({
  info(...args: any[]) {
    log && console.log("TOBSDB:INFO", ...args);
  },
  debug(...args: any[]) {
    log && debug && console.log("TOBSDB:DEBUG", ...args);
  },
  error(...args: any[]) {
    log && console.log("TOBSDB:ERROR", ...args);
  },
  warn(...args: any[]) {
    log && console.log("TOBSDB:WARN", ...args);
  },
});
