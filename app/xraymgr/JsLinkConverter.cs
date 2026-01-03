sing System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;



using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace v2rayManager
{
    /// <summary>
    /// مبدّل لینک→JSON با یک پردازه‌ی Node پایدار.
    /// خروجی = دقیقا خروجی جاوااسکریپت (Outbound.fromLink(...).toJson())، بدون دستکاری.
    /// </summary>
    public sealed class JsLinkConverter : IAsyncDisposable, IDisposable
    {
        // 🧩 پیش‌فرض‌ها داخل خود کلاس
        private const string DefaultNodePath = @"c:\Program Files\nodejs\node.exe";
        private const string DefaultScriptsDir = "webbundle";
        private const int DefaultTimeoutMs = 15000;

        private readonly string _scriptsDir;
        private readonly string? _nodePath;
        private Process? _proc;
        private string? _bridgePath;
        private readonly SemaphoreSlim _ioLock = new(1, 1);
        private bool _disposed;

        /// <summary>از پیش‌فرض‌های داخلی استفاده می‌کند.</summary>
        public JsLinkConverter() : this(DefaultScriptsDir, DefaultNodePath) { }

        /// <summary>در صورت نیاز می‌تونی مسیرها را Override کنی؛ اگر null بدهی، پیش‌فرض‌ها اعمال می‌شوند.</summary>
        public JsLinkConverter(string? scriptsDir, string? nodePath)
        {
            _scriptsDir = Path.GetFullPath(string.IsNullOrWhiteSpace(scriptsDir) ? DefaultScriptsDir : scriptsDir);
            if (!Directory.Exists(_scriptsDir)) throw new DirectoryNotFoundException(_scriptsDir);

            _nodePath = string.IsNullOrWhiteSpace(nodePath) ? DefaultNodePath : nodePath;
            StartProcess(); // یک‌بار روشن
        }

        private void StartProcess()
        {
            KillProcessSilently();

            _bridgePath = Path.Combine(Path.GetTempPath(), $"v2ray_node_bridge_{Guid.NewGuid():N}.js");
            File.WriteAllText(_bridgePath!, BuildBridgeJs(), new UTF8Encoding(false));

            var psi = new ProcessStartInfo
            {
                FileName = ResolveNode(_nodePath),
                Arguments = $"\"{_bridgePath}\"",
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
#if NET6_0_OR_GREATER
                StandardOutputEncoding = new UTF8Encoding(false),
                StandardErrorEncoding = new UTF8Encoding(false),
#endif
            };

            // مسیر باندل وب
            psi.Environment["WEB_BUNDLE_DIR"] = _scriptsDir;
            // اگر لاگ دیباگ نمی‌خواهی، این را نگذار یا مقدار 0 بده:
            // psi.Environment["BRIDGE_DEBUG"] = "0";

            _proc = new Process { StartInfo = psi, EnableRaisingEvents = false };
            if (!_proc.Start())
                throw new InvalidOperationException("Failed to start Node.js bridge.");

            // ❗ بسیار مهم: تخلیهٔ غیرهمزمان stderr برای جلوگیری از پر شدن بافر و بلاک شدن Node
            try
            {
                _proc.ErrorDataReceived += (_, __) => { /* drain & discard to prevent pipe blocking */ };
                _proc.BeginErrorReadLine();
            }
            catch
            {
                // اگر محیط اجازه نداد، بی‌اثر. ولی در اکثر موارد فعال می‌شود.
            }

            // منتظر READY از بریج (stdout را همچنان خط‌به‌خط می‌خوانیم)
            var ready = _proc.StandardOutput.ReadLine();
            if (!string.Equals(ready, "READY", StringComparison.Ordinal))
                throw new InvalidOperationException("Node bridge did not signal READY.");
        }


        private static string ResolveNode(string? nodePath)
        {
            if (!string.IsNullOrWhiteSpace(nodePath) && File.Exists(nodePath)) return nodePath;

            var pathEnv = Environment.GetEnvironmentVariable("PATH") ?? "";
            foreach (var dir in pathEnv.Split(Path.PathSeparator))
            {
                var d = dir.Trim(' ', '"');
                if (string.IsNullOrEmpty(d)) continue;
                var p = Path.Combine(d, "node.exe");
                if (File.Exists(p)) return p;
                p = Path.Combine(d, "node"); // Linux/macOS
                if (File.Exists(p)) return p;
            }
            var candidates = new[]
            {
                @"C:\Program Files\nodejs\node.exe",
                @"C:\Program Files (x86)\nodejs\node.exe",
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "Programs","nodejs","node.exe"),
                "/usr/bin/node", "/usr/local/bin/node"
            };
            foreach (var p in candidates) if (File.Exists(p)) return p;
            return nodePath ?? "node";
        }

        /// <summary>تبدیل لینک (vmess/vless/...)؛ خروجی = toJson اورجینال JS. null یعنی نامعتبر/خطا.</summary>
        public Task<string?> ConvertAsync(string link, CancellationToken ct = default)
            => ConvertAsync(link, DefaultTimeoutMs, ct);

        /// <summary>با timeout سفارشی.</summary>
        public async Task<string?> ConvertAsync(string link, int timeoutMs, CancellationToken ct)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(JsLinkConverter));
            if (string.IsNullOrWhiteSpace(link)) return null;

            await _ioLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (_proc is null || _proc.HasExited)
                    StartProcess();

                // ورودی (UTF-8 بدون BOM)
#if NET6_0_OR_GREATER
                var normalized = link.ReplaceLineEndings(" ");
#else
                var normalized = link.Replace("\r", " ").Replace("\n", " ");
#endif
                var bytes = new UTF8Encoding(false).GetBytes(normalized + "\n");
#if NET6_0_OR_GREATER
                await _proc!.StandardInput.BaseStream.WriteAsync(bytes, 0, bytes.Length, ct);
                await _proc!.StandardInput.BaseStream.FlushAsync(ct);
#else
                await _proc!.StandardInput.BaseStream.WriteAsync(bytes, 0, bytes.Length);
                await _proc!.StandardInput.BaseStream.FlushAsync();
#endif

                // خواندن یک خط خروجی با timeout
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(timeoutMs);

                var readTask = _proc.StandardOutput.ReadLineAsync();
#if NET6_0_OR_GREATER
                using (cts.Token.Register(() => { try { _proc.Kill(entireProcessTree: true); } catch { } }))
#else
                using (cts.Token.Register(() => { try { _proc.Kill(); } catch { } }))
#endif
                {
                    var line = await readTask.ConfigureAwait(false);

                    if (_proc.HasExited && string.IsNullOrEmpty(line)) { StartProcess(); return null; }
                    if (line is null) return null;

                    line = line.Trim();
                    if (line.StartsWith("ERR:", StringComparison.Ordinal)) return null;
                    if (string.IsNullOrWhiteSpace(line) || line.Equals("null", StringComparison.OrdinalIgnoreCase)) return null;

                    return line; // ✅ دقیقا همان JSONی که JS چاپ کرده
                }
            }
            finally
            {
                _ioLock.Release();
            }
        }

        private void KillProcessSilently()
        {
            try
            {
                if (_proc != null && !_proc.HasExited)
                {
#if NET6_0_OR_GREATER
                    _proc.Kill(entireProcessTree: true);
#else
                    _proc.Kill();
#endif
                    _proc.WaitForExit(2000);
                }
            }
            catch { /* ignore */ }
            try { _proc?.Dispose(); } catch { }
            _proc = null;

            try { if (!string.IsNullOrEmpty(_bridgePath) && File.Exists(_bridgePath)) File.Delete(_bridgePath); } catch { }
            _bridgePath = null;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            KillProcessSilently();
            _ioLock.Dispose();
        }

        public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }

        // ---------- اسکریپت Nodeِ ماندگار (line-based) = خروجی toJson اورجینال ----------
        private static string BuildBridgeJs() => @"
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const readline = require('readline');

const BUNDLE_DIR = process.env.WEB_BUNDLE_DIR || process.cwd();

// Polyfills حداقلی، فقط اگر نبودند:
function polyfill() {
  if (typeof global.ObjectUtil === 'undefined') {
    global.ObjectUtil = {
      isEmpty: v => v === undefined || v === null || v === '',
      isArrEmpty: a => !Array.isArray(a) || a.length === 0
    };
  }
  if (typeof global.Base64 === 'undefined') {
    global.Base64 = {
      decode: s => {
        s = String(s).replace(/-/g, '+').replace(/_/g, '/');
        const pad = s.length % 4;
        if (pad === 2) s += '==';
        else if (pad === 3) s += '=';
        else if (pad !== 0) s += '==';
        return Buffer.from(s, 'base64').toString('utf8');
      }
    };
  }
  if (typeof global.atob === 'undefined') {
    global.atob = s => Buffer.from(String(s), 'base64').toString('utf8');
  }
  if (typeof global.Wireguard === 'undefined') {
    global.Wireguard = { generateKeypair: sk => ({ publicKey: '' }) };
  }
  global.data = undefined;
}

function runFile(p) { const code = fs.readFileSync(p, 'utf8'); vm.runInThisContext(code, { filename: p }); }
function runFirstExisting(list) {
  for (const rp of list) {
    const p = path.join(BUNDLE_DIR, rp);
    if (fs.existsSync(p)) { runFile(p); return true; }
  }
  return false;
}
function findAndRunDeep(rootDir, targetName) {
  const stack = [rootDir];
  while (stack.length) {
    const dir = stack.pop();
    let ents;
    try { ents = fs.readdirSync(dir, { withFileTypes: true }); } catch { continue; }
    for (const e of ents) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) stack.push(p);
      else if (e.isFile() && e.name.toLowerCase() === targetName.toLowerCase()) { runFile(p); return true; }
    }
  }
  return false;
}

polyfill();
// utils (اختیاری)
runFirstExisting(['assets/js/util/common.js','js/util/common.js','util/common.js','common.js']);
runFirstExisting(['assets/js/util/utils.js','js/util/utils.js','util/utils.js','utils.js']);
runFirstExisting(['assets/js/util/date-util.js','js/util/date-util.js','util/date-util.js','date-util.js']);
runFirstExisting(['assets/js/util/wireguard.js','js/util/wireguard.js','util/wireguard.js','wireguard.js']);
// outbound.js (اجباری)
if (!(runFirstExisting(['assets/js/model/outbound.js','js/model/outbound.js','model/outbound.js','outbound.js'])
   || findAndRunDeep(BUNDLE_DIR, 'outbound.js'))) {
  console.error('outbound.js not found in WEB_BUNDLE_DIR');
  process.stdout.write('ERR:OUTBOUND_NOT_FOUND\n');
  process.exit(2);
}

// سیگنال آماده
process.stdout.write('READY\n');

// هر خط ورودی → یک خط خروجی (بدون دستکاری)
const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on('line', (line) => {
  try {
    line = String(line || '').trim();
    if (!line) { process.stdout.write('null\n'); return; }
    const out = Outbound.fromLink(line);
    const json = out ? (out.toJson ? out.toJson() : out) : null;
    const text = json ? JSON.stringify(json) : 'null';
    process.stdout.write(text + '\n');
  } catch (err) {
    console.error(err && err.stack ? err.stack : String(err));
    process.stdout.write('ERR:EX\n');
  }
});
";
    }
}