import {promises as fs} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {Ledger} from './service.js';
import {auditTransactions, stateAtSequence, lineageGraph} from './provenance.js';
import {inspectBackup, restoreBackup, compareBackupToStore} from './restore.js';
import {rollingSummary, dailyCoverage, thresholdRuns} from './resampling.js';
import {listen} from './http.js';
import {StreamManager} from './streamManager.js';
import {errorDocument, fail} from './errors.js';
import {jsonParse, integer} from './value.js';
import {runImport, replayImportPlan} from './pipeline.js';
import {operationsReport, regionReport, sourceScorecard} from './reports.js';

export function argumentsFor(argv) {
  const [command = 'help', ...rest] = argv;
  const options = {};
  for (let index = 0; index < rest.length; index += 2) {
    const key = rest[index];
    const value = rest[index + 1];
    if (!key.startsWith('--') || value === undefined || Object.hasOwn(options, key.slice(2))) {
      fail('ARGUMENTS', 'Options require unique --name value pairs');
    }
    options[key.slice(2)] = value;
  }
  const allowed = ['root', 'input', 'id', 'actor', 'port', 'destination', 'expected-sequence', 'files-root', 'preview', 'url', 'manager'];
  for (const key of Object.keys(options)) {
    if (!allowed.includes(key)) {
      fail('ARGUMENTS', `Unknown option: ${key}`);
    }
  }
  return {command, options};
}
async function readInput(options) {
  if (!options.input) {
    return {};
  }
  const stat = await fs.stat(options.input);
  if (stat.size > 16 * 1024 * 1024) {
    fail('INPUT_LIMIT', 'Input file is too large');
  }
  return jsonParse(await fs.readFile(options.input, 'utf8'));
}
export async function execute(argv) {
  const {command, options} = argumentsFor(argv);
  if (command === 'help') {
    return {
      usage: 'node src/cli.js COMMAND --root DIRECTORY --input JSON_FILE',
      commands: [
        'source', 'source-update', 'station', 'station-update', 'ingest', 'preview',
        'query', 'history', 'timeline', 'watermark', 'quarantine', 'resolve',
        'windows', 'gaps', 'rule', 'rule-disable', 'evaluate', 'alerts', 'acknowledge',
        'export', 'export-get', 'export-manifest', 'export-revoke', 'profile', 'spikes',
        'flatlines', 'disagreements', 'freshness', 'inventory', 'reconcile', 'impact',
        'status', 'catalog', 'batches', 'audit', 'verify', 'checkpoint', 'backup',
        'journal-audit', 'historical-status', 'lineage', 'restore', 'backup-inspect', 'backup-compare', 'rolling', 'coverage', 'runs', 'recover-lock', 'import', 'import-plan', 'operations', 'region', 'scorecard', 'serve',
        'stream-register', 'stream-pause', 'stream-resume', 'stream-stop', 'stream-reconfigure', 'stream-status', 'stream-runtime', 'stream-deadletters', 'stream-reprocess', 'stream-run',
      ],
      rules: 'All input dates include explicit timezone. State remains under --root. No downloads are required.',
    };
  }
  if (!options.root) {
    fail('ARGUMENTS', '--root is required');
  }
  const ledger = new Ledger(options.root);
  const input = ['journal-audit', 'historical-status', 'lineage', 'restore', 'backup-inspect', 'backup-compare'].includes(command) ? {} : await readInput(options);
  const actor = options.actor ?? 'cli';
  const expectedSequence = options['expected-sequence'] === undefined ? undefined :
    integer(Number(options['expected-sequence']), 'expected-sequence', 0);
  switch (command) {
    case 'journal-audit':
      await ledger.store.read();
      return auditTransactions((await ledger.store.replay()).transactions);
    case 'historical-status':
      await ledger.store.read();
      return stateAtSequence((await ledger.store.replay()).transactions, Number(options.id));
    case 'lineage':
      return lineageGraph(await ledger.store.read(), options.id);
    case 'restore':
      return restoreBackup(options.input, options.destination);
    case 'backup-inspect':
      return (await inspectBackup(options.input)).manifest;
    case 'backup-compare':
      return compareBackupToStore(options.input, options.root);
    case 'rolling':
      return rollingSummary(await ledger.store.read(), input);
    case 'coverage':
      return dailyCoverage(await ledger.store.read(), input);
    case 'runs':
      return thresholdRuns(await ledger.store.read(), input);
    case 'source':
      return ledger.registerSource(input, actor);
    case 'source-update':
      return ledger.updateSource(options.id, input, actor);
    case 'station':
      return ledger.registerStation(input, actor);
    case 'station-update':
      return ledger.updateStation(options.id, input, actor);
    case 'ingest':
      return ledger.ingest(input, actor, {expectedSequence});
    case 'preview':
      return ledger.preview(input);
    case 'query':
      return ledger.query(input);
    case 'history':
      return ledger.history(options.id);
    case 'timeline':
      return ledger.timeline(input);
    case 'watermark':
      return ledger.watermark(input, actor);
    case 'quarantine':
      return ledger.quarantine(input);
    case 'resolve':
      return ledger.resolve(input, actor);
    case 'windows':
      return ledger.windows(input);
    case 'gaps':
      return ledger.gaps(input);
    case 'rule':
      return ledger.saveRule(input, actor);
    case 'rule-disable':
      return ledger.disableRule(options.id, actor);
    case 'evaluate':
      return ledger.evaluate(input, actor);
    case 'alerts':
      return ledger.alerts(input);
    case 'acknowledge':
      return ledger.acknowledge(input, actor);
    case 'export':
      return ledger.export(input, actor);
    case 'export-get':
      return ledger.getExport(options.id);
    case 'export-manifest':
      return ledger.exportManifest(options.id);
    case 'export-revoke':
      return ledger.revokeExport(input, actor);
    case 'profile':
      return ledger.profile(input);
    case 'spikes':
      return ledger.spikes(input);
    case 'flatlines':
      return ledger.flatlines(input);
    case 'disagreements':
      return ledger.disagreements(input);
    case 'freshness':
      return ledger.freshness(input);
    case 'inventory':
      return ledger.inventory(input);
    case 'reconcile':
      return ledger.reconcile(input);
    case 'impact':
      return ledger.impact(input);
    case 'status':
      return ledger.status();
    case 'catalog':
      return ledger.catalog();
    case 'batches':
      return ledger.batches(input);
    case 'audit':
      return ledger.audit(input);
    case 'verify':
      return ledger.verify();
    case 'checkpoint':
      return ledger.checkpoint();
    case 'backup':
      if (!options.destination) {
        fail('ARGUMENTS', '--destination is required');
      }
      return ledger.backup(options.destination);
    case 'recover-lock':
      return ledger.recoverLock();
    case 'import':
      return runImport(ledger, options['files-root'] ?? path.dirname(options.input), input,
        {preview: options.preview === 'true', actor, expectedSequence});
    case 'import-plan':
      return replayImportPlan(ledger, options['files-root'] ?? path.dirname(options.input), input);
    case 'operations':
      return operationsReport(await ledger.store.read(), input);
    case 'region':
      return regionReport(await ledger.store.read(), input);
    case 'scorecard':
      return sourceScorecard(await ledger.store.read(), options.id);
    case 'serve': {
      const manager = options.manager === 'true' ? new StreamManager(ledger) : null;
      if (manager) {
        await manager.start();
      }
      const server = await listen(ledger, {port: Number(options.port ?? 8080), token: process.env.FIELD_LEDGER_TOKEN, manager});
      const stop = () => {
        server.close();
        manager?.close();
      };
      process.once('SIGINT', stop);
      process.once('SIGTERM', stop);
      return {listening: server.address(), authentication: Boolean(process.env.FIELD_LEDGER_TOKEN),
        streamManager: Boolean(manager)};
    }
    case 'stream-register': {
      // 一次性命令：只持久化订阅配置，连接循环由 serve/stream-run 承载。
      const manager = new StreamManager(ledger, {detached: true});
      return manager.register(input);
    }
    case 'stream-pause':
      return new StreamManager(ledger, {detached: true}).pause(options.id);
    case 'stream-resume':
      return new StreamManager(ledger, {detached: true}).resume(options.id);
    case 'stream-stop':
      return new StreamManager(ledger, {detached: true}).stop(options.id);
    case 'stream-reconfigure':
      return new StreamManager(ledger, {detached: true}).reconfigure(options.id, input);
    case 'stream-reprocess':
      return ledger.reprocessDeadLetter(input, actor);
    case 'stream-deadletters':
      return ledger.streamDeadLetters({...input, streamId: input.streamId ?? options.id});
    case 'stream-status':
      return ledger.streamStatus(options.id);
    case 'stream-runtime':
      return new StreamManager(ledger, {detached: true}).status(options.id);
    case 'stream-run': {
      // 独立进程驱动一次订阅，直到进入 idle/error/paused/stopped，或位置稳定 settleMs。
      const manager = new StreamManager(ledger);
      const id = options.id ?? input.id;
      const timeoutMs = Number(input.timeoutMs ?? 60000);
      const settleMs = Number(input.settleMs ?? 1500);
      try {
        const existing = await ledger.streamStatus(id);
        if (existing.status === 'error' || existing.status === 'idle') {
          // 上次运行留下的可恢复状态：显式恢复后再启动连接循环。
          await manager.resume(id);
        }
        await manager.start();
        const durable = await manager.waitFor(id, (state, runtime) =>
          ['idle', 'error', 'paused', 'stopped'].includes(state.status) ||
          runtime.phase === 'idle' || runtime.phase === 'error',
          {timeoutMs, settleMs});
        return {stream: durable, runtime: manager.status(id)};
      } finally {
        await manager.close();
      }
    }
    default:
      fail('COMMAND', `Unknown command: ${command}`);
  }
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const result = await execute(process.argv.slice(2));
    process.stdout.write(JSON.stringify(result, null, 2) + String.fromCharCode(10));
  } catch (error) {
    process.stderr.write(JSON.stringify(errorDocument(error)) + String.fromCharCode(10));
    process.exitCode = 1;
  }
}
