public static class ConfigDuplicateMarker
{
    private const int BatchSize = 1000;

    // شمارنده‌ها برای Watch (اختیاری)
    static int CandidatesRead;     // تعداد کاندیدهای خوانده‌شده
    static int GroupsBuilt;        // تعداد گروه‌های ساخته‌شده
    static int PrimariesMarked;    // تعداد primary ها
    static int DuplicatesMarked;   // تعداد duplicate ها
    static int Writes;             // تعداد آپدیت‌های اعمال‌شده

    /// <summary>اجرای فرایند داپلیکیت‌یابی. پارامتری لازم ندارد.</summary>
    public static async Task<int> MarkDuplicatesAsync(CancellationToken ct = default)
    {
        // Reset counters
        Interlocked.Exchange(ref CandidatesRead, 0);
        Interlocked.Exchange(ref GroupsBuilt, 0);
        Interlocked.Exchange(ref PrimariesMarked, 0);
        Interlocked.Exchange(ref DuplicatesMarked, 0);
        Interlocked.Exchange(ref Writes, 0);

        var sw = Stopwatch.StartNew();

        var conn = SqliteManager.conn;
        if (conn.State != ConnectionState.Open) conn.Open();

        // --- 1) فقط کاندیدها را بخوان ---
        var rows = new List<(long id, string json)>(capacity: 4096);
        using (var cmd = new SQLiteCommand(
            "SELECT id, json FROM configs " +
            "WHERE corrupt = 0 " +
            "  AND (json IS NOT NULL AND LENGTH(TRIM(json)) > 0) " +
            "  AND dup_check = 0 AND dublicate = 0 " +
            "ORDER BY id ASC;", conn))
        using (var r = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
        {
            while (r.Read())
            {
                ct.ThrowIfCancellationRequested();
                long id = r.GetInt64(0);
                string json = r.IsDBNull(1) ? "" : r.GetString(1);
                rows.Add((id, json));
                Interlocked.Increment(ref CandidatesRead);
            }
        }

        if (rows.Count == 0)
        {
            Console.WriteLine("هیچ کاندیدی برای داپلیکیت‌یابی یافت نشد.");
            return 0;
        }

        // --- 2) نرمال‌سازی و گروه‌بندی فقط بین همین کاندیدها ---
        var groups = new Dictionary<string, List<long>>(rows.Count);
        foreach (var (id, json) in rows)
        {
            ct.ThrowIfCancellationRequested();

            if (!TryCanonicalHash(json, out var hash))
            {
                // اگر JSON پارس نشد، از raw-trim هش بگیر تا از گروه‌سازی جا نماند
                hash = FastSha256Hex(Encoding.UTF8.GetBytes((json ?? "").Trim()));
            }

            if (!groups.TryGetValue(hash, out var list))
            {
                list = new List<long>(4);
                groups.Add(hash, list);
            }
            list.Add(id);
        }
        Interlocked.Exchange(ref GroupsBuilt, groups.Count);

        // --- 3) تصمیم‌گیری گروه‌ها ---
        var primaries = new List<(long id, long gid)>(rows.Count); // gid=primaryId یا 0 (تک‌عضوی)
        var duplicates = new List<(long id, long gid)>(rows.Count); // gid=primaryId

        foreach (var kv in groups)
        {
            ct.ThrowIfCancellationRequested();

            var ids = kv.Value;
            if (ids.Count == 0) continue;

            ids.Sort(); // کمترین id → primary

            if (ids.Count == 1)
            {
                primaries.Add((ids[0], 0)); // گروه تک‌عضوی
            }
            else
            {
                var primaryId = ids[0];
                primaries.Add((primaryId, primaryId));
                for (int i = 1; i < ids.Count; i++)
                    duplicates.Add((ids[i], primaryId));
            }
        }

        Interlocked.Exchange(ref PrimariesMarked, primaries.Count);
        Interlocked.Exchange(ref DuplicatesMarked, duplicates.Count);

        // --- 4) اعمال به DB به صورت Batch ---
        using var cmdPrimary = new SQLiteCommand(
            "UPDATE configs SET dup_check = 1, dublicate = 0, dup_group_id = @gid " +
            "WHERE id = @id AND dup_check = 0 AND dublicate = 0;", conn);
        var pPrimId = cmdPrimary.Parameters.Add("@id", DbType.Int64);
        var pPrimGid = cmdPrimary.Parameters.Add("@gid", DbType.Int64);

        using var cmdDup = new SQLiteCommand(
            "UPDATE configs SET dup_check = 1, dublicate = 1, dup_group_id = @gid " +
            "WHERE id = @id AND dup_check = 0 AND dublicate = 0;", conn);
        var pDupId = cmdDup.Parameters.Add("@id", DbType.Int64);
        var pDupGid = cmdDup.Parameters.Add("@gid", DbType.Int64);

        void FlushPrimRange(List<(long id, long gid)> list, int start, int count)
        {
            if (count <= 0) return;
            using var tx = conn.BeginTransaction();
            cmdPrimary.Transaction = tx;

            for (int k = 0; k < count; k++)
            {
                var (id, gid) = list[start + k];
                pPrimId.Value = id;
                pPrimGid.Value = gid; // 0 برای تک‌عضوی؛ یا id پرایمری
                cmdPrimary.ExecuteNonQuery();
                Interlocked.Increment(ref Writes);
            }
            tx.Commit();
        }

        void FlushDupRange(List<(long id, long gid)> list, int start, int count)
        {
            if (count <= 0) return;
            using var tx = conn.BeginTransaction();
            cmdDup.Transaction = tx;

            for (int k = 0; k < count; k++)
            {
                var (id, gid) = list[start + k];
                pDupId.Value = id;
                pDupGid.Value = gid; // id پرایمری
                cmdDup.ExecuteNonQuery();
                Interlocked.Increment(ref Writes);
            }
            tx.Commit();
        }

        for (int i = 0; i < primaries.Count; i += BatchSize)
        {
            ct.ThrowIfCancellationRequested();
            int len = Math.Min(BatchSize, primaries.Count - i);
            FlushPrimRange(primaries, i, len);
        }
        for (int i = 0; i < duplicates.Count; i += BatchSize)
        {
            ct.ThrowIfCancellationRequested();
            int len = Math.Min(BatchSize, duplicates.Count - i);
            FlushDupRange(duplicates, i, len);
        }

        sw.Stop();
        Console.WriteLine($"✅ Duplicates marked. Candidates={CandidatesRead}, Groups={GroupsBuilt}, Primaries={PrimariesMarked}, Dups={DuplicatesMarked}, Writes={Writes} در {sw.Elapsed.TotalSeconds:F2} ثانیه.");
        return Writes;
    }

    // ----------------- Canonicalization helpers -----------------

    private static bool TryCanonicalHash(string json, out string hashHex)
    {
        hashHex = string.Empty;
        try
        {
            using var doc = JsonDocument.Parse(json, new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip
            });
            var utf8 = CanonicalizeToUtf8(doc.RootElement);
            hashHex = FastSha256Hex(utf8);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static byte[] CanonicalizeToUtf8(JsonElement root)
    {
        var buf = new ArrayBufferWriter<byte>(root.GetRawText().Length + 64);
        using (var w = new Utf8JsonWriter(buf, new JsonWriterOptions
        {
            Indented = false,
            SkipValidation = false
        }))
        {
            WriteCanonical(w, root, isRoot: true);
        }
        return buf.WrittenSpan.ToArray();
    }

    private static void WriteCanonical(Utf8JsonWriter w, JsonElement el, bool isRoot)
    {
        switch (el.ValueKind)
        {
            case JsonValueKind.Object:
                {
                    var props = new List<(string Name, JsonElement Val)>();
                    foreach (var p in el.EnumerateObject())
                    {
                        if (isRoot && string.Equals(p.Name, "tag", StringComparison.Ordinal))
                            continue; // حذف tag فقط در ریشه
                        props.Add((p.Name, p.Value));
                    }
                    props.Sort((a, b) => string.CompareOrdinal(a.Name, b.Name));

                    w.WriteStartObject();
                    foreach (var (name, val) in props)
                    {
                        w.WritePropertyName(name);
                        WriteCanonical(w, val, isRoot: false);
                    }
                    w.WriteEndObject();
                    break;
                }

            case JsonValueKind.Array:
                w.WriteStartArray();
                foreach (var it in el.EnumerateArray())
                    WriteCanonical(w, it, isRoot: false);
                w.WriteEndArray();
                break;

            case JsonValueKind.String:
                w.WriteStringValue(el.GetString());
                break;

            case JsonValueKind.Number:
                {
                    if (el.TryGetInt64(out long l)) { w.WriteNumberValue(l); break; }
                    var raw = el.GetRawText();
                    if (decimal.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var dec))
                        w.WriteNumberValue(dec);
                    else if (double.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var d))
                        w.WriteNumberValue(d);
                    else
                        w.WriteRawValue(raw); // fallback
                    break;
                }

            case JsonValueKind.True: w.WriteBooleanValue(true); break;
            case JsonValueKind.False: w.WriteBooleanValue(false); break;
            case JsonValueKind.Null: w.WriteNullValue(); break;

            default:
                w.WriteRawValue(el.GetRawText());
                break;
        }
    }

    private static string FastSha256Hex(ReadOnlySpan<byte> bytes)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(bytes.ToArray());
        return Convert.ToHexString(hash);
    }

    private static string FastSha256Hex(byte[] bytes)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(bytes);
        return Convert.ToHexString(hash);
    }
}