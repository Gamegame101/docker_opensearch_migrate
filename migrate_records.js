#!/usr/bin/env node

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

// Configuration
const BATCH_SIZE = 1000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;

// Initialize Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
  db: { schema: 'api' }
});

// Parse ad_date text → { ad_date: 'YYYY-MM-DD', active_time_hr: number }
function parseAdDate(adDateText) {
  if (!adDateText) return { ad_date: null, active_time_hr: null };

  const dateMatch = adDateText.match(/Started running on (\d{1,2} \w{3} \d{4})/);
  const timeMatch = adDateText.match(/Total active time (\d+) hrs/);

  let ad_date = null;
  let active_time_hr = null;

  if (dateMatch) {
    try {
      const d = new Date(dateMatch[1]);
      if (!isNaN(d.getTime())) {
        ad_date = d.toISOString().split('T')[0]; // YYYY-MM-DD
      }
    } catch (e) {
      ad_date = null;
    }
  }

  if (timeMatch) {
    active_time_hr = parseInt(timeMatch[1], 10);
  }

  return { ad_date, active_time_hr };
}

// Transform a source row to destination schema
function transformRow(row) {
  const { ad_date, active_time_hr } = parseAdDate(row.ad_date);

  return {
    id: row.id,
    keyword: row.keyword || null,
    ad_id: row.ad_id ? parseInt(row.ad_id, 10) || null : null,
    ad_url: row.ad_url || null,
    page_url: row.page_url || null,
    ad_date: ad_date,
    active_time_hr: active_time_hr,
    ad_name: row.ad_name || null,
    ad_profile_url: row.ad_profile_url || null,
    ad_caption: row.ad_caption || null,
    ad_links: row.ad_links || null,
    image_urls: row.image_urls || null,
    video_urls: row.video_urls || null,
    collected_at: row.collected_at || null,
    ad_risk_level: row.ad_risk_level ? parseInt(row.ad_risk_level, 10) || null : null,
    ad_risk_reason: row.ad_risk_reason || null,
    request_id: row.request_id ? parseInt(row.request_id, 10) || null : null,
    opensearch_sync: false
  };
}

async function main() {
  console.log('🚀 Starting migration (pageseeker_response → pageseeker_response_opensearch)...');
  console.log('🎯 Mode: UPSERT (insert new + update existing, reset opensearch_sync=false)');
  console.log(`📊 Batch size: ${BATCH_SIZE}`);
  console.log('='.repeat(60));

  const startTime = Date.now();
  let totalProcessed = 0;
  let totalUpserted = 0;
  let totalErrors = 0;
  let lastId = 0;

  // ดึงจำนวน records จาก source
  const { count: sourceCount, error: countErr } = await supabase
    .from('pageseeker_response')
    .select('id', { count: 'exact', head: true });

  if (countErr) {
    console.error('❌ Error counting source records:', countErr.message);
    process.exit(1);
  }

  console.log(`📊 Source table (pageseeker_response): ${sourceCount?.toLocaleString() || '?'} records`);

  // ดึงจำนวน records จาก destination
  const { count: destCount } = await supabase
    .from('pageseeker_response_opensearch')
    .select('id', { count: 'exact', head: true });

  console.log(`📊 Dest table (pageseeker_response_opensearch): ${destCount?.toLocaleString() || '?'} records`);
  console.log('='.repeat(60));

  while (true) {
    // ดึง batch จาก source table โดยใช้ cursor-based pagination (id > lastId)
    const { data: rows, error: fetchErr } = await supabase
      .from('pageseeker_response')
      .select('id, keyword, ad_id, ad_url, page_url, ad_date, ad_name, ad_profile_url, ad_caption, ad_links, image_urls, video_urls, collected_at, ad_risk_level, ad_risk_reason, request_id')
      .gt('id', lastId)
      .order('id', { ascending: true })
      .limit(BATCH_SIZE);

    if (fetchErr) {
      console.error(`❌ Error fetching batch (lastId=${lastId}):`, fetchErr.message);
      totalErrors++;
      // ลอง skip batch นี้
      lastId += BATCH_SIZE;
      if (totalErrors > 10) {
        console.error('❌ Too many errors, stopping');
        break;
      }
      continue;
    }

    if (!rows || rows.length === 0) {
      console.log('✅ No more records to migrate');
      break;
    }

    // Transform ทุก row
    const transformed = rows.map(transformRow);

    // Upsert batch
    const { error: upsertErr } = await supabase
      .from('pageseeker_response_opensearch')
      .upsert(transformed, {
        onConflict: 'id',
        ignoreDuplicates: false
      });

    if (upsertErr) {
      console.error(`❌ Upsert error (lastId=${lastId}):`, upsertErr.message);
      totalErrors++;
    } else {
      totalUpserted += transformed.length;
    }

    totalProcessed += rows.length;
    lastId = rows[rows.length - 1].id;

    // Progress
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = totalProcessed / elapsed;
    const pct = sourceCount ? ((totalProcessed / sourceCount) * 100).toFixed(1) : '?';
    console.log(`📊 Progress: ${totalProcessed.toLocaleString()}/${sourceCount?.toLocaleString()} (${pct}%) | ${rate.toFixed(0)} rec/s | lastId=${lastId}`);
  }

  const elapsed = (Date.now() - startTime) / 1000;

  console.log('\n' + '='.repeat(60));
  console.log('🎉 MIGRATION completed!');
  console.log(`📊 Records processed: ${totalProcessed.toLocaleString()}`);
  console.log(`✅ Records upserted: ${totalUpserted.toLocaleString()}`);
  console.log(`❌ Errors: ${totalErrors}`);
  if (totalProcessed > 0) {
    console.log(`🎯 Success rate: ${((totalUpserted / totalProcessed) * 100).toFixed(1)}%`);
  }
  console.log(`⏱️  Duration: ${elapsed.toFixed(1)}s`);
  if (elapsed > 0) {
    console.log(`⚡ Speed: ${(totalProcessed / elapsed).toFixed(0)} records/s`);
  }

  // ดึงจำนวน records หลัง migrate
  const { count: finalCount } = await supabase
    .from('pageseeker_response_opensearch')
    .select('id', { count: 'exact', head: true });

  console.log(`📊 Dest table after migration: ${finalCount?.toLocaleString() || '?'} records`);

  if (totalErrors > 0) {
    console.log('⚠️  Migration completed with errors');
    process.exit(1);
  }

  console.log('🏆 MIGRATION SUCCESSFUL!');
  process.exit(0);
}

main().catch(err => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
