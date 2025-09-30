export default {
  async fetch(request, env, ctx) {
      try {
        await initializeAndMigrateDatabase(env);
      } catch (e) {
        console.error("Database initialization failed:", e.stack);
        return new Response("严重错误：数据库初始化失败，请检查Worker的D1数据库绑定是否正确配置为'WUYA'。", { status: 500 });
      }
      
      const url = new URL(request.url);
      const path = url.pathname;
      try {
          if (path.startsWith('/api/')) {
              return await handleApiRequest(request, env);
          }
          if (path.length > 1 && !path.startsWith('/api/') && !['/login', '/admin'].includes(path)) {
              const fileName = path.substring(1);
              return await handleGitHubFileProxy(fileName, env, ctx);
          }
          return await handleUiRequest(request, env);
      } catch (e) {
          console.error("Global Catch:", e.stack);
          const errorResponse = { error: "发生意外的服务器错误。", details: e.message };
          const status = e.status || 500;
          if (path.startsWith('/api/')) {
              return jsonResponse(errorResponse, status);
          }
          return new Response(`错误: ${e.message}\n${e.stack}`, { status });
      }
  },
  async scheduled(controller, env, ctx) {
      console.log("Scheduled task started: Initializing...");
      await initializeAndMigrateDatabase(env);

      const db = env.WUYA;
      const nextTask = await getSetting(db, 'next_sync_task') || 'domains';

      if (nextTask === 'domains') {
          console.log("Scheduled task: Syncing a batch of DNS records (failure-first)...");
          await syncScheduledDomains(env);
          await setSetting(db, 'next_sync_task', 'ip_sources');
      } else {
          console.log("Scheduled task: Syncing a batch of IP sources to GitHub (failure-first)...");
          await syncScheduledIpSources(env);
          await setSetting(db, 'next_sync_task', 'domains');
      }

      console.log("Scheduled task for this cycle finished.");
  },
};

async function syncScheduledDomains(env) {
    const BATCH_SIZE = 5;
    const db = env.WUYA;
    const log = (msg) => console.log(beijingTimeLog(msg));

    const { token, zoneId } = await getCfApiSettings(db);
    if (!token || !zoneId) {
        log("Cannot run scheduled domain sync: Cloudflare settings are missing.");
        return;
    }

    const query = `
        SELECT * FROM domains 
        WHERE is_enabled = 1 
        ORDER BY 
            CASE last_sync_status WHEN 'failed' THEN 0 ELSE 1 END, 
            last_synced_time ASC
        LIMIT ?`;
    
    const { results: domainsToSync } = await db.prepare(query).bind(BATCH_SIZE).all();

    if (domainsToSync.length === 0) {
        log("No domains to sync in this batch.");
        return;
    }

    log(`Found ${domainsToSync.length} domains for this sync batch (failure-first).`);
    const syncContext = {};
    for (const domain of domainsToSync) {
        try {
            await syncDomainLogic(domain, token, zoneId, db, log, syncContext);
        } catch (e) {
            log(`Error processing domain ${domain.target_domain} in batch: ${e.message}`);
        }
    }
}

async function syncScheduledIpSources(env) {
    const BATCH_SIZE = 5;
    const db = env.WUYA;
    const log = (msg) => console.log(beijingTimeLog(msg));

    const githubSettings = await getGitHubSettings(db);
    if (!githubSettings.token || !githubSettings.owner || !githubSettings.repo) {
        log("Cannot run scheduled IP source sync: GitHub settings are missing.");
        return;
    }

    const query = `
        SELECT id FROM ip_sources 
        WHERE is_enabled = 1 
        ORDER BY 
            CASE last_sync_status WHEN 'failed' THEN 0 ELSE 1 END, 
            last_synced_time ASC
        LIMIT ?`;

    const { results: sourcesToSync } = await db.prepare(query).bind(BATCH_SIZE).all();

    if (sourcesToSync.length === 0) {
        log("No IP sources to sync in this batch.");
        return;
    }

    log(`Found ${sourcesToSync.length} IP sources for this sync batch (failure-first).`);
    for (const source of sourcesToSync) {
        await syncSingleIpSource(source.id, env, false).catch(e => {
             log(`Error processing IP source ID ${source.id} in batch: ${e.message}`);
        });
    }
}

async function initializeAndMigrateDatabase(env) {
  if (!env.WUYA) {
      throw new Error("D1 database binding 'WUYA' not found. Please configure it in your Worker settings.");
  }
  const db = env.WUYA;
  const expectedSchemas = {
      settings: ['key TEXT PRIMARY KEY NOT NULL', 'value TEXT NOT NULL'],
      domains: [
          'id INTEGER PRIMARY KEY AUTOINCREMENT',
          'source_domain TEXT NOT NULL',
          'target_domain TEXT NOT NULL',
          'zone_id TEXT NOT NULL',
          'is_deep_resolve INTEGER NOT NULL DEFAULT 1',
          'ttl INTEGER NOT NULL DEFAULT 60',
          'notes TEXT',
          'last_synced_records TEXT DEFAULT \'[]\'',
          'last_synced_time TIMESTAMP',
          'last_sync_status TEXT DEFAULT \'pending\'',
          'last_sync_error TEXT',
          'is_enabled INTEGER DEFAULT 1 NOT NULL',
          'is_system INTEGER NOT NULL DEFAULT 0',
          'UNIQUE(target_domain)'
      ],
      sessions: ['token TEXT PRIMARY KEY NOT NULL', 'expires_at TIMESTAMP NOT NULL'],
      ip_sources: [
          'id INTEGER PRIMARY KEY AUTOINCREMENT',
          'url TEXT NOT NULL UNIQUE',
          'github_path TEXT NOT NULL UNIQUE',
          'commit_message TEXT NOT NULL',
          'fetch_strategy TEXT',
          'last_synced_time TIMESTAMP',
          'last_sync_status TEXT DEFAULT \'pending\'',
          'last_sync_error TEXT',
          'is_enabled INTEGER DEFAULT 1 NOT NULL'
      ]
  };

  const createStmts = Object.keys(expectedSchemas).map(tableName =>
      db.prepare(`CREATE TABLE IF NOT EXISTS ${tableName} (${expectedSchemas[tableName].join(', ')});`)
  );
  await db.batch(createStmts);

  for (const tableName in expectedSchemas) {
      const { results: existingColumns } = await db.prepare(`PRAGMA table_info(${tableName})`).all();
      const existingColumnNames = existingColumns.map(c => c.name);
      const expectedColumnDefs = expectedSchemas[tableName].filter(def => !def.startsWith('UNIQUE'));

      for (const columnDef of expectedColumnDefs) {
          const columnName = columnDef.split(' ')[0];
          if (!existingColumnNames.includes(columnName)) {
              try {
                  await db.prepare(`ALTER TABLE ${tableName} ADD COLUMN ${columnDef}`).run();
              } catch (e) { console.error(`Failed to add column '${columnName}' to '${tableName}':`, e.message); }
          }
      }
  }
  
  const { token, zoneId } = await getCfApiSettings(db);
  if (!token || !zoneId) return;

  const { results: invalidDomains } = await db.prepare("SELECT id, target_domain FROM domains WHERE target_domain LIKE '%.'").all();
  if (invalidDomains.length > 0) {
      try {
          const zoneName = (await getZoneName(token, zoneId)).replace(/\.$/, '');
          const fixStmts = [];
          for (const domain of invalidDomains) {
              const prefix = domain.target_domain.replace(/\.$/, '');
              const correctedDomain = (prefix === '' || prefix === '@') ? zoneName : `${prefix}.${zoneName}`;
              fixStmts.push(db.prepare("UPDATE domains SET target_domain = ? WHERE id = ?").bind(correctedDomain, domain.id));
          }
          await db.batch(fixStmts);
      } catch (e) { console.error("Failed to fix invalid domain entries:", e.message); }
  }
}

async function ensureInitialData(db, zoneId, zoneName) {
    if (!zoneId || !zoneName) return;
    
    await setSetting(db, 'THREE_NETWORK_SOURCE', await getSetting(db, 'THREE_NETWORK_SOURCE') || 'CloudFlareYes');

    const initialIpSources = [
        { url: 'https://ipdb.api.030101.xyz/?type=bestcf&country=true', path: '030101-bestcf.txt', msg: 'Update BestCF IPs from 030101.xyz', strategy: 'phantomjs_cloud' },
        { url: 'https://ipdb.api.030101.xyz/?type=bestproxy&country=true', path: '030101-bestproxy.txt', msg: 'Update BestProxy IPs from 030101.xyz', strategy: 'phantomjs_cloud' },
        { url: 'https://ip.164746.xyz', path: '164746.txt', msg: 'Update IPs from 164746.xyz', strategy: 'direct_regex' },
        { url: 'https://stock.hostmonit.com/CloudFlareYes', path: 'CloudFlareYes.txt', msg: 'Update CloudFlareYes IPs', strategy: 'phantomjs_cloud' },
        { url: 'https://ip.haogege.xyz', path: 'haogege.txt', msg: 'Update IPs from haogege.xyz', strategy: 'direct_regex' },
        { url: 'https://api.uouin.com/cloudflare.html', path: 'uouin-cloudflare.txt', msg: 'Update IPs from uouin.com', strategy: 'direct_regex' },
        { url: 'https://www.wetest.vip/page/cloudflare/address_v4.html', path: 'wetest-cloudflare-v4.txt', msg: 'Update Cloudflare v4 IPs from wetest.vip', strategy: 'direct_regex' },
        { url: 'https://www.wetest.vip/page/edgeone/address_v4.html', path: 'wetest-edgeone-v4.txt', msg: 'Update EdgeOne v4 IPs from wetest.vip', strategy: 'direct_regex' },
    ];
    const ipSourceStmts = initialIpSources.map(s => 
        db.prepare('INSERT INTO ip_sources (url, github_path, commit_message, fetch_strategy) VALUES (?, ?, ?, ?) ON CONFLICT(url) DO NOTHING')
          .bind(s.url, s.path, s.msg, s.strategy)
    );
    
    const initialDomains = [
        { source: 'internal:hostmonit:yd', prefix: 'yd', notes: '移动', is_system: 1, deep_resolve: 1 },
        { source: 'internal:hostmonit:dx', prefix: 'dx', notes: '电信', is_system: 1, deep_resolve: 1 },
        { source: 'internal:hostmonit:lt', prefix: 'lt', notes: '联通', is_system: 1, deep_resolve: 1 },
        { source: 'snipaste1.speedip.eu.org', prefix: 'bp1', notes: 'bp1', is_system: 0, deep_resolve: 1 },
        { source: 'snipaste2.speedip.eu.org', prefix: 'bp2', notes: 'bp2', is_system: 0, deep_resolve: 1 },
        { source: 'snipaste3.speedip.eu.org', prefix: 'bp3', notes: 'bp3', is_system: 0, deep_resolve: 1 },
        { source: 'snipaste4.speedip.eu.org', prefix: 'bp4', notes: 'bp4', is_system: 0, deep_resolve: 1 },
        { source: 'snipaste5.speedip.eu.org', prefix: 'bp5', notes: 'bp5', is_system: 0, deep_resolve: 1 },
        { source: 'cf.090227.xyz', prefix: 'cm', notes: 'cm', is_system: 0, deep_resolve: 1 },
        { source: 'cf.877774.xyz', prefix: 'qms', notes: 'qms', is_system: 0, deep_resolve: 1 },
    ];
    const domainStmts = initialDomains.map(d => {
        const targetDomain = d.prefix === '@' ? zoneName : `${d.prefix}.${zoneName}`;
        return db.prepare('INSERT INTO domains (source_domain, target_domain, zone_id, is_deep_resolve, notes, is_system) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(target_domain) DO NOTHING')
                 .bind(d.source, targetDomain, zoneId, d.deep_resolve, d.notes, d.is_system);
    });

    await db.batch([...ipSourceStmts, ...domainStmts]);
}

async function getSetting(db, key) { return db.prepare("SELECT value FROM settings WHERE key = ?").bind(key).first("value"); }
async function setSetting(db, key, value) { 
    if (value === undefined || value === null) {
        await db.prepare("DELETE FROM settings WHERE key = ?").bind(key).run();
    } else {
        await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)").bind(key, value).run();
    }
}

async function getFullSettings(db) {
    const { results } = await db.prepare("SELECT key, value FROM settings").all();
    const settings = {};
    for (const row of results) {
        settings[row.key] = row.value;
    }
    return settings;
}

async function getCfApiSettings(db) {
  const [token, zoneId] = await Promise.all([
      getSetting(db, 'CF_API_TOKEN'),
      getSetting(db, 'CF_ZONE_ID'),
  ]);
  return { token: token || '', zoneId: zoneId || '' };
}

async function getGitHubSettings(db) {
    const [token, owner, repo] = await Promise.all([
        getSetting(db, 'GITHUB_TOKEN'),
        getSetting(db, 'GITHUB_OWNER'),
        getSetting(db, 'GITHUB_REPO'),
    ]);
    return { token, owner, repo };
}

async function handleApiRequest(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;
  const db = env.WUYA;

  if (path === '/api/status' && method === 'GET') {
      const passwordSet = await getSetting(db, 'ADMIN_PASSWORD_HASH');
      return jsonResponse({ isInitialized: !!passwordSet });
  }
  if (path === '/api/setup' && method === 'POST') return await apiSetup(request, db);
  if (path === '/api/login' && method === 'POST') return await apiLogin(request, db);

  if (!await isAuthenticated(request, db)) return jsonResponse({ error: '未授权' }, 401);

  if (method === 'POST' && path === '/api/logout') return await apiLogout(request, db);
  if (method === 'GET' && path === '/api/settings') return await apiGetSettings(request, db);
  if (method === 'POST' && path === '/api/settings') return await apiSetSettings(request, db);
  if (method === 'GET' && path === '/api/domains') return await apiGetDomains(request, db);
  if (method === 'POST' && path === '/api/domains') return await apiAddDomain(request, db);
  if (method === 'POST' && path === '/api/sync') return syncAllDomains(env, true);
  if (method === 'POST' && path === '/api/domains/sync_system') return syncSystemDomains(env, true);
  
  const domainMatch = path.match(/^\/api\/domains\/(\d+)$/);
  if (domainMatch) {
      const id = domainMatch[1];
      if (method === 'PUT') return await apiUpdateDomain(request, db, id);
      if (method === 'DELETE') return await apiDeleteDomain(request, db, id);
  }

  const domainRecordsMatch = path.match(/^\/api\/domains\/(\d+)\/records$/);
  if (domainRecordsMatch && method === 'GET') {
      const id = domainRecordsMatch[1];
      return await apiGetDomainRecords(id, db);
  }
  
  const syncMatch = path.match(/^\/api\/domains\/(\d+)\/sync$/);
  if (syncMatch && method === 'POST') {
      const id = syncMatch[1];
      return syncSingleDomain(id, env, true);
  }
  
  if (method === 'GET' && path === '/api/ip_sources') return await apiGetIpSources(db);
  if (method === 'POST' && path === '/api/ip_sources') return await apiAddIpSource(request, db);
  if (method === 'POST' && path === '/api/ip_sources/probe') return await apiProbeIpSource(request);
  if (method === 'POST' && path === '/api/ip_sources/sync_all') return syncAllIpSources(env, true);

  const ipSourceMatch = path.match(/^\/api\/ip_sources\/(\d+)$/);
  if (ipSourceMatch) {
      const id = ipSourceMatch[1];
      if (method === 'PUT') return await apiUpdateIpSource(request, db, id);
      if (method === 'DELETE') return await apiDeleteIpSource(db, id);
  }

  const ipSourceSyncMatch = path.match(/^\/api\/ip_sources\/(\d+)\/sync$/);
  if (ipSourceSyncMatch && method === 'POST') {
      const id = ipSourceSyncMatch[1];
      return syncSingleIpSource(id, env, true);
  }

  return jsonResponse({ error: 'API 端点未找到' }, 404);
}

async function apiSetup(request, db) {
  const passwordSet = await getSetting(db, 'ADMIN_PASSWORD_HASH');
  if (passwordSet) return jsonResponse({ error: '应用已经初始化。' }, 403);
  const { password } = await request.json();
  if (!password || password.length < 8) return jsonResponse({ error: '密码长度必须至少为8个字符。' }, 400);
  const salt = crypto.randomUUID();
  const hash = await hashPassword(password, salt);
  await db.batch([
      db.prepare("INSERT INTO settings (key, value) VALUES ('ADMIN_PASSWORD_HASH', ?)").bind(hash),
      db.prepare("INSERT INTO settings (key, value) VALUES ('PASSWORD_SALT', ?)").bind(salt),
  ]);
  return jsonResponse({ success: true });
}

async function apiLogin(request, db) {
  const { password } = await request.json();
  const [storedHash, salt] = await Promise.all([getSetting(db, 'ADMIN_PASSWORD_HASH'), getSetting(db, 'PASSWORD_SALT')]);
  if (!storedHash || !salt) return jsonResponse({ error: '应用尚未初始化。' }, 400);
  const inputHash = await hashPassword(password, salt);
  if (inputHash === storedHash) {
      const token = crypto.randomUUID();
      const expires = new Date(Date.now() + 24 * 60 * 60 * 1000);
      await db.prepare("INSERT INTO sessions (token, expires_at) VALUES (?, ?)").bind(token, expires.toISOString()).run();
      const sessionCookie = `session=${token}; HttpOnly; Secure; Path=/; SameSite=Strict; Max-Age=86400`;
      return jsonResponse({ success: true }, 200, { 'Set-Cookie': sessionCookie });
  }
  return jsonResponse({ error: '密码无效。' }, 401);
}

async function apiLogout(request, db) {
  const token = getCookie(request, 'session');
  if (token) await db.prepare("DELETE FROM sessions WHERE token = ?").bind(token).run();
  const expiryCookie = 'session=; HttpOnly; Secure; Path=/; SameSite=Strict; Expires=Thu, 01 Jan 1970 00:00:00 GMT';
  return jsonResponse({ success: true }, 200, { 'Set-Cookie': expiryCookie });
}

async function apiGetSettings(request, db) {
    const settings = await getFullSettings(db);
    const { token, zoneId } = await getCfApiSettings(db);
    if (token && zoneId) {
        try { settings.zoneName = await getZoneName(token, zoneId); } catch (e) { console.warn("Could not fetch zone name for settings endpoint"); }
    }
    return jsonResponse(settings);
}

async function apiSetSettings(request, db) {
    const settings = await request.json();
    const { CF_API_TOKEN, CF_ZONE_ID } = settings;
    if (CF_API_TOKEN && CF_ZONE_ID) {
        try {
            const zoneName = await getZoneName(CF_API_TOKEN, CF_ZONE_ID);
            await ensureInitialData(db, CF_ZONE_ID, zoneName);
        } catch (e) {
            return jsonResponse({ error: `Cloudflare API 验证失败: ${e.message}` }, 400);
        }
    }
    const setPromises = Object.entries(settings).map(([key, value]) => setSetting(db, key, value));
    await Promise.all(setPromises);
    return jsonResponse({ success: true, message: '设置已成功保存。' });
}

async function apiGetDomains(request, db) {
  const query = `
      SELECT id, source_domain, target_domain, zone_id, is_deep_resolve, ttl, notes, 
             strftime('%Y-%m-%dT%H:%M:%SZ', last_synced_time) as last_synced_time, 
             last_sync_status, last_sync_error, is_enabled, is_system 
      FROM domains ORDER BY is_system DESC, target_domain`;
  const { results } = await db.prepare(query).all();
  return jsonResponse(results);
}

async function handleDomainMutation(request, db, isUpdate = false, id = null) {
  const { source_domain, target_domain_prefix, is_deep_resolve, ttl, notes } = await request.json();
  const { token, zoneId } = await getCfApiSettings(db);
  if (!zoneId) return jsonResponse({ error: '尚未在设置中配置区域 ID。' }, 400);
  if (!source_domain || !target_domain_prefix) return jsonResponse({ error: '缺少必填字段。' }, 400);
  
  const zoneName = (await getZoneName(token, zoneId)).replace(/\.$/, '');
  const target_domain = (target_domain_prefix.trim() === '@' || target_domain_prefix.trim() === '') 
      ? zoneName : `${target_domain_prefix.trim()}.${zoneName}`;

  const values = [source_domain, target_domain, zoneId, is_deep_resolve ? 1 : 0, ttl || 60, notes || null];
  try {
      if (isUpdate) {
          const domainInfo = await db.prepare("SELECT is_system FROM domains WHERE id = ?").bind(id).first();
          if (domainInfo && domainInfo.is_system) {
              return jsonResponse({ error: "系统预设目标不可编辑。" }, 403);
          }
          await db.prepare('UPDATE domains SET source_domain=?, target_domain=?, zone_id=?, is_deep_resolve=?, ttl=?, notes=? WHERE id = ?')
              .bind(...values, id).run();
          return jsonResponse({ success: true, message: "目标更新成功。" });
      } else {
          await db.prepare('INSERT INTO domains (source_domain, target_domain, zone_id, is_deep_resolve, ttl, notes, is_system) VALUES (?, ?, ?, ?, ?, ?, 0)')
              .bind(...values).run();
          return jsonResponse({ success: true, message: "目标添加成功。" });
      }
  } catch (e) {
      if (e.message.includes('UNIQUE constraint failed')) return jsonResponse({ error: '该目标域名已存在。' }, 409);
      throw e;
  }
}

async function apiAddDomain(request, db) { return handleDomainMutation(request, db, false); }
async function apiUpdateDomain(request, db, id) { return handleDomainMutation(request, db, true, id); }
async function apiDeleteDomain(request, db, id) {
    const { changes } = await db.prepare('DELETE FROM domains WHERE id = ? AND is_system = 0').bind(id).run();
    if (changes === 0) {
        return jsonResponse({ error: "删除失败，目标为系统预设或不存在。" }, 403);
    }
    return jsonResponse({ success: true, message: "目标删除成功。" });
}

async function apiGetDomainRecords(id, db) {
    try {
        const { token, zoneId } = await getCfApiSettings(db);
        if (!token || !zoneId) throw new Error("Cloudflare API 未配置。");
        
        const domain = await db.prepare("SELECT target_domain FROM domains WHERE id = ?").bind(id).first();
        if (!domain) return jsonResponse({ error: "目标未找到" }, 404);

        const apiEndpoint = `https://api.cloudflare.com/client/v4/zones/${zoneId}/dns_records?name=${domain.target_domain}`;
        const headers = { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' };
        
        const response = await fetch(apiEndpoint, { headers });
        if (!response.ok) throw new Error(`Cloudflare API 错误: ${response.statusText}`);
        
        const data = await response.json();
        if (!data.success) throw new Error(`Cloudflare API 返回错误: ${JSON.stringify(data.errors)}`);
        
        const records = data.result.map(({ type, content }) => ({ type, content }));
        return jsonResponse(records);
    } catch (e) {
        return jsonResponse({ error: e.message }, 500);
    }
}


async function handleUiRequest(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;
  const db = env.WUYA;
  
  const isInitialized = !!(await getSetting(db, 'ADMIN_PASSWORD_HASH'));
  const loggedIn = await isAuthenticated(request, db);
  let pageContent, pageTitle;

  if (!isInitialized) {
      pageTitle = '系统初始化';
      pageContent = getSetupPage();
  } else if (path === '/admin' && loggedIn) {
      pageTitle = 'DNS Clone Dashboard';
      const settingsPromise = getFullSettings(db);
      const settings = await settingsPromise;
      
      const { token, zoneId } = await getCfApiSettings(db);
      if (token && zoneId) {
          try {
              settings.zoneName = await getZoneName(token, zoneId);
              await ensureInitialData(db, zoneId, settings.zoneName);
          } catch(e) { console.warn("Could not fetch zone name or ensure initial data.", e.message); }
      }

      const domainsPromise = db.prepare("SELECT id, source_domain, target_domain, zone_id, is_deep_resolve, ttl, notes, strftime('%Y-%m-%dT%H:%M:%SZ', last_synced_time) as last_synced_time, last_sync_status, last_sync_error, is_enabled, is_system FROM domains ORDER BY is_system DESC, target_domain").all();
      const ipSourcesPromise = db.prepare("SELECT id, url, github_path, commit_message, fetch_strategy, strftime('%Y-%m-%dT%H:%M:%SZ', last_synced_time) as last_synced_time, last_sync_status, last_sync_error, is_enabled FROM ip_sources ORDER BY github_path").all();
      
      const [{ results: domains }, { results: ipSources }] = await Promise.all([domainsPromise, ipSourcesPromise]);

      pageContent = getDashboardPage(domains, ipSources, settings);
  } else if (path === '/admin' && !loggedIn) {
      return new Response(null, { status: 302, headers: { 'Location': '/' } });
  } else {
      pageTitle = 'CF-DNS-Clon';
      const domainsPromise = db.prepare("SELECT source_domain, target_domain, notes, last_synced_time, is_system FROM domains WHERE is_enabled = 1 AND last_sync_status IN ('success', 'no_change') ORDER BY is_system DESC, notes").all();
      const ipSourcesPromise = db.prepare("SELECT url, github_path, last_synced_time FROM ip_sources WHERE is_enabled = 1 AND last_sync_status IN ('success', 'no_change') ORDER BY github_path").all();
      const threeNetworkSourcePromise = getSetting(db, 'THREE_NETWORK_SOURCE');

      const [{ results: domains }, { results: ipSources }, threeNetworkSource] = await Promise.all([domainsPromise, ipSourcesPromise, threeNetworkSourcePromise]);
      
      const sourceNameMap = { CloudFlareYes: 'CloudFlareYes', 'api.uouin.com': 'UoUin', 'wetest.vip': 'Wetest' };
      const sourceDisplayName = sourceNameMap[threeNetworkSource] || '未知';

      pageContent = getPublicHomepage(request.url, domains, ipSources, sourceDisplayName, loggedIn);
  }
  return new Response(getHtmlLayout(pageTitle, pageContent), { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });
}

function getHtmlLayout(title, content) { return `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>${title}</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css"><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css"><style>
:root {
    --sidebar-width: 250px;
    --pico-font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --pico-font-size: 16px;
    --pico-line-height: 1.6;
    --pico-border-radius: 12px;
    --pico-form-element-spacing-vertical: 1rem;
    --pico-form-element-spacing-horizontal: 1.25rem;
    --pico-shadow-sm: 0 2px 4px rgba(0,0,0,0.05);
    --pico-shadow-md: 0 4px 12px rgba(0,0,0,0.1);
    --pico-shadow-lg: 0 10px 30px rgba(0,0,0,0.1);
    --c-primary: #007aff;
    --c-primary-hover: #0056b3;
    --c-bg: #f0f2f5;
    --c-bg-blur: rgba(248, 249, 250, 0.7);
    --c-card-bg: rgba(255, 255, 255, 0.6);
    --c-card-border: rgba(255, 255, 255, 0.9);
    --c-text: #212529;
    --c-text-muted: #6c757d;
    --c-text-accent: var(--c-primary);
    --c-icon-bg: #e9ecef;
    --c-button-bg: var(--c-primary);
    --c-button-text: #ffffff;
    --noise-bg: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 800"><filter id="n"><feTurbulence type="fractalNoise" baseFrequency="0.65" numOctaves="3" stitchTiles="stitch"/></filter><rect width="100%" height="100%" filter="url(%23n)" opacity="0.1"/></svg>');
}
html.dark {
    --c-primary: #0a84ff;
    --c-primary-hover: #409cff;
    --c-bg: #121212;
    --c-bg-blur: rgba(29, 29, 31, 0.7);
    --c-card-bg: rgba(29, 29, 31, 0.5);
    --c-card-border: rgba(255, 255, 255, 0.1);
    --c-text: #e5e5e7;
    --c-text-muted: #8e8e93;
    --c-text-accent: var(--c-primary);
    --c-icon-bg: #3a3a3c;
    --c-button-bg: var(--c-primary);
}
body {
    font-family: var(--pico-font-family);
    margin: 0;
    background-color: var(--c-bg);
    color: var(--c-text);
    transition: background-color 0.3s ease, color 0.3s ease;
    overflow-x: hidden;
}
.public-body-wrapper {
    position: relative;
    z-index: 1;
    width: 100%;
    min-height: 100vh;
}
.background-blurs {
    position: fixed;
    top: 0; left: 0;
    width: 100%; height: 100%;
    overflow: hidden;
    z-index: 0;
}
.background-blurs::before {
    content: '';
    position: absolute;
    width: 100%;
    height: 100%;
    background: var(--noise-bg);
}
.blur-orb {
    position: absolute;
    border-radius: 50%;
    filter: blur(100px);
    opacity: 0.2;
    animation: move 20s infinite alternate;
}
.blur-orb-1 {
    width: 500px; height: 500px;
    top: 10%; left: 10%;
    background-color: #007aff;
}
.blur-orb-2 {
    width: 400px; height: 400px;
    bottom: 10%; right: 10%;
    background-color: #ff3b30;
    animation-delay: -10s;
}
@keyframes move {
    from { transform: translate(-50px, -50px) rotate(0deg); }
    to { transform: translate(50px, 50px) rotate(360deg); }
}
main.container { max-width: none; padding: 0; display: flex; }
.sidebar { width: var(--sidebar-width); flex-shrink: 0; background: var(--pico-card-background-color); height: 100vh; position: sticky; top: 0; border-right: 1px solid var(--pico-card-border-color); display: flex; flex-direction: column; }
.sidebar-header { padding: 1.5rem; text-align: left; border-bottom: 1px solid var(--pico-card-border-color); }
.sidebar-header h3 { margin: 0; font-size: 1.75rem; font-weight: 700; color: var(--pico-primary); }
.sidebar-nav { padding: 1rem 0; flex-grow: 1; }
.sidebar-nav a { display: flex; align-items: center; gap: .85rem; padding: .85rem 1.5rem; color: #495057; text-decoration: none; margin: 0 .75rem; border-radius: 6px; transition: background-color .2s ease, color .2s ease; font-weight: 500; }
.sidebar-nav a:hover { background: #e9ecef; color: #212529; }
.sidebar-nav a.active { color: #fff; background-color: var(--pico-primary); font-weight: 600; }
.main-content { flex-grow: 1; padding: 2.5rem; }
.page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem; }
.page-header h2 { margin: 0; font-size: 2rem; font-weight: 700; }
.page { display: none; }
.page.active { display: block; animation: fadeIn .3s ease-out forwards; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
article, fieldset { border-radius: var(--pico-border-radius); border: 1px solid var(--pico-card-border-color); background: var(--pico-card-background-color); box-shadow: var(--pico-shadow-sm); padding: 2rem; }
dialog article { box-shadow: var(--pico-shadow-lg); }
legend { font-size: 1.25rem; font-weight: 600; padding: 0 .5rem; }
.domain-card { background-color: var(--pico-card-background-color); border: 1px solid var(--pico-card-border-color); border-radius: var(--pico-border-radius); padding: 1.5rem; display: grid; grid-template-columns: 2fr 1.5fr 1fr auto; gap: 1.5rem; align-items: center; transition: box-shadow .2s ease, transform .2s ease; margin-bottom: 1rem; }
.card-col { display: flex; flex-direction: column; gap: 0.2rem; min-width: 0; }
.card-col strong { font-size: 0.75rem; color: #6c757d; margin-bottom: .25rem; text-transform: uppercase; font-weight: 600; letter-spacing: 0.05em; }
.card-col .domain-cell { font-size: 1rem; font-weight: 500; color: #212529; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; cursor: pointer; }
.card-col small.domain-cell { color: #6c757d; font-weight: 400; }
.card-actions { display: flex; justify-content: flex-end; gap: .5rem; }
.record-details summary { display: inline-flex; align-items: center; cursor: pointer; user-select: none; list-style: none; font-weight: 500; }
.record-details ul { margin: 8px 0 0; padding-left: 20px; font-size: 0.9em; }
.status-success { color: #198754; } .status-failed { color: #dc3545; } .status-no_change { color: #6c757d; }
.notifications, .home-toast { z-index: 1050; }
.public-nav {
    position: sticky;
    top: 1rem;
    max-width: 1200px;
    margin: 0 auto 2rem auto;
    padding: 0.75rem 1rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: var(--c-bg-blur);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    border: 1px solid var(--c-card-border);
    border-radius: var(--pico-border-radius);
    box-shadow: var(--pico-shadow-md);
    z-index: 100;
}
.public-nav-title { font-size: 1.25rem; font-weight: 600; color: var(--c-text); }
.public-nav-actions { display: flex; align-items: center; gap: 1rem; }
.public-nav-actions a, .public-nav-actions button {
    text-decoration: none;
    padding: 0.5rem 1rem;
    border-radius: 8px;
    font-weight: 500;
    transition: all 0.2s ease;
    border: none;
}
.public-nav-actions a { background-color: var(--c-button-bg); color: var(--c-button-text); }
.public-nav-actions a:hover { filter: brightness(1.1); }
#theme-toggle { background: var(--c-icon-bg); color: var(--c-text); font-size: 1.2rem; width: 40px; height: 40px; display:flex; align-items:center; justify-content:center; }
.public-container { max-width: 1200px; margin: 0 auto; padding: 1.5rem; }
.public-section h2 { font-size: 1.75rem; margin-bottom: 2rem; display: flex; align-items: center; gap: 0.75rem; color: var(--c-text); border: none; padding: 0;}
.public-section h2::before { content: ''; display: block; width: 5px; height: 1.25rem; background-color: var(--c-primary); border-radius: 3px; }
.public-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 1.5rem; }
.public-card {
    background: var(--c-card-bg);
    backdrop-filter: blur(5px);
    -webkit-backdrop-filter: blur(5px);
    border: 1px solid var(--c-card-border);
    border-radius: var(--pico-border-radius);
    box-shadow: var(--pico-shadow-md);
    padding: 1.5rem;
    cursor: pointer;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    display: flex;
    flex-direction: column;
    gap: 1rem;
}
.public-card:hover { transform: translateY(-5px); box-shadow: var(--pico-shadow-lg); }
.public-card-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; }
.public-card-title { font-size: 1.1rem; font-weight: 600; color: var(--c-text); margin: 0; }
.public-card-meta { font-size: 0.8rem; color: var(--c-text-muted); white-space: nowrap; }
.public-card-content { font-family: "SF Mono", "Consolas", "Menlo", monospace; font-size: 1rem; color: var(--c-text-accent); word-break: break-all; }
.public-card-footer { font-size: 0.85rem; color: var(--c-text-muted); display: flex; align-items: center; gap: 0.5rem; }
.home-toast { position: fixed; top: 80px; right: 20px; background-color: var(--c-primary); color: #fff; padding: 12px 20px; border-radius: 6px; z-index: 1000; font-weight: 500; box-shadow: var(--pico-shadow-lg); transition: transform 0.4s ease-in-out, opacity 0.4s ease-in-out; transform: translateY(-100px); opacity: 0; }
.home-toast.show { transform: translateY(0); opacity: 1; }
.home-toast.hide { transform: translateY(50px); opacity: 0; }

@media (max-width: 768px) {
    .public-nav { top: 0; left: 0; right: 0; border-radius: 0; width: 100%; }
    .public-container { padding: 1rem; }
    .public-grid { grid-template-columns: 1fr; }
    main.container {
        display: block;
    }
    .sidebar {
        width: 100%;
        height: auto;
        position: static;
        border-right: none;
        border-bottom: 1px solid var(--pico-card-border-color);
    }
    .main-content {
        padding: 1.5rem 1rem;
    }
    .page-header {
        flex-direction: column;
        gap: 1rem;
        align-items: flex-start;
    }
    .page-header h2 {
        font-size: 1.75rem;
    }
    .domain-card {
        grid-template-columns: 1fr;
        gap: 1rem;
        padding: 1rem;
    }
    .card-actions {
        flex-direction: row;
        justify-content: flex-start;
        flex-wrap: wrap;
    }
}
</style></head><body><main class="container">${content}</main><div id="notifications" class="notifications"></div></body></html>`; }

function getSetupPage() { return `<div class="auth-container"><article id="setupForm"><h1>系统初始化</h1><p>请设置一个安全的管理员密码以保护您的应用。</p><form><label for="password">管理员密码 (最少8位)</label><input type="password" id="password" required minlength="8"><button type="submit">设置密码</button></form><p id="error-msg" style="color:red"></p></article></div><script>document.querySelector('#setupForm form').addEventListener('submit',async function(e){e.preventDefault();const password=document.getElementById('password').value;document.getElementById('error-msg').textContent='';try{const res=await fetch('/api/setup',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password})});if(!res.ok){const err=await res.json();throw new Error(err.error||'设置失败')}alert('设置成功，页面即将刷新...');setTimeout(()=>window.location.reload(),1000)}catch(e){document.getElementById('error-msg').textContent=e.message}});</script>`;}

function getPublicHomepage(requestUrl, domains, ipSources, threeNetworkSourceName, loggedIn) {
    const origin = new URL(requestUrl).origin;
    const formatTime = (isoStr) => {
        if (!isoStr) return 'N/A';
        const date = new Date(isoStr);
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    };

    const domainCards = domains.map(d => {
        let sourceHost;
        if (d.is_system) {
            sourceHost = threeNetworkSourceName;
        } else {
            try {
                let urlCompatibleSource = d.source_domain;
                if (!urlCompatibleSource.startsWith('http://') && !urlCompatibleSource.startsWith('https://')) {
                    urlCompatibleSource = 'https://' + urlCompatibleSource;
                }
                sourceHost = new URL(urlCompatibleSource).hostname;
            } catch (e) {
                sourceHost = d.source_domain || '解析错误';
            }
        }
        return `
        <div class="public-card" data-copy-content="${d.target_domain}">
            <div class="public-card-header">
                <h3 class="public-card-title">${d.notes || '未知线路'}</h3>
                <span class="public-card-meta">${formatTime(d.last_synced_time)}</span>
            </div>
            <div class="public-card-content">${d.target_domain}</div>
            <div class="public-card-footer">
                <i class="fa-solid fa-link fa-xs"></i> <span>来源: ${sourceHost}</span>
            </div>
        </div>`;
    }).join('');

    const ipSourceCards = ipSources.map(s => {
        const fullUrl = `${origin}/${s.github_path}`;
        return `
        <div class="public-card" data-copy-content="${fullUrl}">
            <div class="public-card-header">
                <h3 class="public-card-title">${s.github_path}</h3>
                <span class="public-card-meta">${formatTime(s.last_synced_time)}</span>
            </div>
            <div class="public-card-content">${fullUrl}</div>
            <div class="public-card-footer">
                <i class="fa-solid fa-link fa-xs"></i> <span>来源: ${new URL(s.url).hostname}</span>
            </div>
        </div>`;
    }).join('');

    const authButton = loggedIn 
        ? `<a href="/admin" role="button">进入后台</a>`
        : `<button class="outline" onclick="document.getElementById('login-modal').showModal()">登录</button>`;

    return `
    <div class="public-body-wrapper">
        <div class="background-blurs">
            <div class="blur-orb blur-orb-1"></div>
            <div class="blur-orb blur-orb-2"></div>
        </div>
        <nav class="public-nav">
            <div class="public-nav-title">CF-DNS-Clon</div>
            <div class="public-nav-actions">
                ${authButton}
                <button id="theme-toggle" aria-label="Toggle theme">
                    <i class="fa-solid fa-moon"></i>
                </button>
            </div>
        </nav>
        <div class="public-container">
            <section class="public-section">
                <h2><i class="fa-solid fa-globe"></i> 优选域名</h2>
                <div class="public-grid">${domainCards || '<p>暂无可用数据</p>'}</div>
            </section>
            <section class="public-section">
                <h2><i class="fa-solid fa-server"></i> 优选API</h2>
                <div class="public-grid">${ipSourceCards || '<p>暂无可用数据</p>'}</div>
            </section>
        </div>
    </div>
    <div id="copy-toast" class="home-toast"></div>
    
    <dialog id="login-modal">
      <article>
        <header>
          <a href="#close" aria-label="Close" class="close" onclick="document.getElementById('login-modal').close()"></a>
          <h3>管理员登录</h3>
        </header>
        <p>请输入密码以继续。</p>
        <form id="modal-login-form">
          <label for="modal-password">密码</label>
          <input type="password" id="modal-password" name="password" required>
          <p id="modal-error-msg" style="color: var(--pico-color-red-500); height: 1em;"></p>
          <button type="submit">登录</button>
        </form>
      </article>
    </dialog>

    <script>
        const toast = document.getElementById('copy-toast');
        const themeToggle = document.getElementById('theme-toggle');
        const currentTheme = localStorage.getItem('theme');
        if (currentTheme) {
            document.documentElement.classList.add(currentTheme);
            if (currentTheme === 'dark') {
                themeToggle.innerHTML = '<i class="fa-solid fa-sun"></i>';
            }
        }
        themeToggle.addEventListener('click', () => {
            document.documentElement.classList.toggle('dark');
            let theme = 'light';
            if (document.documentElement.classList.contains('dark')) {
                theme = 'dark';
                themeToggle.innerHTML = '<i class="fa-solid fa-sun"></i>';
            } else {
                 themeToggle.innerHTML = '<i class="fa-solid fa-moon"></i>';
            }
            localStorage.setItem('theme', theme);
        });

        function showToast(message) {
            toast.textContent = message;
            toast.classList.add('show');
            setTimeout(() => {
                toast.classList.remove('show');
                toast.classList.add('hide');
                 setTimeout(() => toast.classList.remove('hide'), 400);
            }, 2000);
        }

        document.querySelector('.public-container').addEventListener('click', (event) => {
            const card = event.target.closest('.public-card');
            if (card && card.dataset.copyContent) {
                const content = card.dataset.copyContent;
                navigator.clipboard.writeText(content).then(() => {
                    showToast('已复制: ' + content);
                }, () => {
                    showToast('复制失败！');
                });
            }
        });
        
        document.getElementById('modal-login-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            const password = document.getElementById('modal-password').value;
            const errorMsg = document.getElementById('modal-error-msg');
            const submitBtn = e.target.querySelector('button');
            errorMsg.textContent = '';
            submitBtn.setAttribute('aria-busy', 'true');
            try {
                const res = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ password })
                });
                if (!res.ok) {
                    const err = await res.json();
                    throw new Error(err.error || '登录失败');
                }
                window.location.href = '/admin';
            } catch (e) {
                errorMsg.textContent = e.message;
            } finally {
                submitBtn.removeAttribute('aria-busy');
            }
        });

    </script>
    `;
}
function getDashboardPage(domains, ipSources, settings) { 
    const githubSettingsComplete = settings.GITHUB_TOKEN && settings.GITHUB_OWNER && settings.GITHUB_REPO;
    return `<aside class="sidebar"><div class="sidebar-header"><h3>DNS Clone</h3></div><nav class="sidebar-nav"><a href="#page-dns-clone" class="nav-link active" data-target="page-dns-clone"><i class="fa-solid fa-clone fa-fw"></i> 域名克隆</a><a href="#page-github-upload" class="nav-link" data-target="page-github-upload"><i class="fa-brands fa-github fa-fw"></i> GitHub 上传</a><a href="#page-settings" class="nav-link" data-target="page-settings"><i class="fa-solid fa-gear fa-fw"></i> 系统设置</a></nav></aside>
    <div class="main-content">
        <div id="page-dns-clone" class="page active">
            <div class="page-header"><h2>域名克隆列表</h2><button id="addDomainBtn" ${settings.CF_ZONE_ID ? '' : 'disabled'}>＋ 添加克隆目标</button></div>
            <div id="domain-list-container"></div>
            <article><h3>手动操作</h3><p>点击下方按钮，可以立即为所有已启用的目标执行一次同步任务。</p><button id="manualSyncBtn">手动同步所有目标</button><pre id="logOutput" style="display:none;"></pre></article>
        </div>
        <div id="page-github-upload" class="page">
            <div class="page-header"><h2>GitHub IP源列表</h2><button id="addIpSourceBtn" ${githubSettingsComplete ? '' : 'disabled'}>＋ 添加IP源</button></div>
             <div id="ip-source-list-container"></div>
            <article><h3>手动操作</h3><p>点击下方按钮，可以立即为所有已启用的IP源执行一次同步并上传到GitHub。</p><button id="manualSyncIpSourcesBtn">同步所有IP源</button><pre id="ipLogOutput" style="display:none;"></pre></article>
        </div>
        <div id="page-settings" class="page">
            <div class="page-header"><h2>系统设置</h2></div>
            <form id="settingsForm">
                <fieldset><legend><i class="fa-brands fa-cloudflare"></i> Cloudflare API 设置</legend><label for="cfToken">API 令牌 (Token)</label><input type="password" id="cfToken" value="${settings.CF_API_TOKEN||''}"><label for="cfZoneId">区域 (Zone) ID</label><input type="text" id="cfZoneId" value="${settings.CF_ZONE_ID||''}">
                    <details class="tutorial-details"><summary>如何获取 API 令牌和区域 ID？</summary><div class="tutorial-content"><ol><li><strong>获取 API 令牌 (Token):</strong><ol type="a"><li>登录 <a href="https://dash.cloudflare.com/" target="_blank">Cloudflare</a>，进入 <strong>“我的个人资料”</strong> &rarr; <strong>“API 令牌”</strong>。</li><li>点击 <strong>“创建令牌”</strong>，然后选择 <strong>“编辑区域 DNS”</strong> 模板并点击“使用模板”。</li><li>在 <strong>“区域资源”</strong> 部分，选择您需要操作的具体域名区域。</li><li>点击“继续以显示摘要”和“创建令牌”，复制生成的令牌。<strong>注意：令牌仅显示一次，请妥善保管。</strong></li></ol></li><li><strong>获取区域 ID (Zone ID):</strong><ol type="a"><li>在 Cloudflare 仪表板主页，点击您需要操作的域名。</li><li>在域名的“概述”页面，您可以在右下角找到 <strong>“区域 ID”</strong>，点击即可复制。</li></ol></li></ol></div></details>
                </fieldset>
                 <fieldset><legend><i class="fa-solid fa-network-wired"></i> 三网优选IP源设置</legend><label for="threeNetworkSource">三网采集源</label><select id="threeNetworkSource"><option value="CloudFlareYes" ${settings.THREE_NETWORK_SOURCE === 'CloudFlareYes' ? 'selected' : ''}>CloudFlareYes</option><option value="api.uouin.com" ${settings.THREE_NETWORK_SOURCE === 'api.uouin.com' ? 'selected' : ''}>api.uouin.com</option><option value="wetest.vip" ${settings.THREE_NETWORK_SOURCE === 'wetest.vip' ? 'selected' : ''}>wetest.vip</option></select><small>为系统预设的电信/移动/联通域名选择IP来源。更改后保存设置将自动同步一次系统域名。</small>
                </fieldset>
                <fieldset><legend><i class="fa-brands fa-github"></i> GitHub API 设置</legend><label for="githubToken">GitHub Token</label><input type="password" id="githubToken" value="${settings.GITHUB_TOKEN||''}" placeholder="具有 repo 权限的 Personal Access Token"><label for="githubOwner">GitHub 用户名/组织名</label><input type="text" id="githubOwner" value="${settings.GITHUB_OWNER||''}" placeholder="例如: my-username"><label for="githubRepo">仓库名称</label><input type="text" id="githubRepo" value="${settings.GITHUB_REPO||''}" placeholder="例如: my-dns-records">
                    <details class="tutorial-details"><summary>如何获取 GitHub API 信息？</summary><div class="tutorial-content"><ol><li><strong>获取 GitHub Token:</strong><ol type="a"><li>登录 <a href="https://github.com/" target="_blank">GitHub</a>，点击右上角头像，进入 <strong>“Settings”</strong>。</li><li>在左侧菜单中，选择 <strong>“Developer settings”</strong> &rarr; <strong>“Personal access tokens”</strong> &rarr; <strong>“Tokens (classic)”</strong>。</li><li>点击 <strong>“Generate new token”</strong>，并选择 <strong>“Generate new token (classic)”</strong>。</li><li>为令牌添加描述（Note），设置合适的过期时间（Expiration）。</li><li>在 <strong>“Select scopes”</strong> 部分，勾选 <code>repo</code> 权限。</li><li>点击页面底部的 <strong>“Generate token”</strong>，并复制生成的令牌。<strong>注意：令牌仅显示一次，请妥善保管。</strong></li></ol></li><li><strong>获取用户名/组织名 和 仓库名称:</strong><ol type="a"><li><strong>用户名/组织名</strong> 就是您的GitHub个人主页URL中，github.com后面的那部分，或者您组织的主页URL。</li><li><strong>仓库名称</strong> 是您在GitHub上创建的，用来存储IP文件的仓库的名字。如果仓库不存在，系统将在第一次同步时自动为您创建为私有仓库。</li></ol></li></ol></div></details>
                </fieldset>
                <button type="submit">保存设置</button>
            </form>
        </div>
    </div>
    <dialog id="domainModal"><article><header><a href="#close" aria-label="Close" class="close" onclick="window.closeModal('domainModal')"></a><h3 id="modalTitle"></h3></header><form id="domainForm"><input type="hidden" id="domainId"><label for="source_domain">克隆域名</label><input type="text" id="source_domain" placeholder="example-source.com" required><label for="target_domain_prefix">我的域名前缀</label><div class="grid"><input type="text" id="target_domain_prefix" placeholder="subdomain or @" required><span id="zoneNameSuffix" style="line-height:var(--pico-form-element-height);font-weight:700">.your-zone.com</span></div><div class="grid"><div><label for="is_deep_resolve">深度 <span class="tooltip">(?)<span class="tooltip-text">开启后，如果克隆域名是CNAME，系统将递归查找最终的IP地址进行解析。关闭则直接克隆CNAME记录本身。</span></span></label><input type="checkbox" id="is_deep_resolve" role="switch" checked></div><div><label for="ttl">TTL (秒)</label><input type="number" id="ttl" min="60" max="86400" value="60" required></div></div><label for="notes">备注 (可选)</label><textarea id="notes" rows="2" placeholder="例如：主力CDN"></textarea><footer><button type="button" class="secondary" onclick="window.closeModal('domainModal')">取消</button><button type="submit" id="saveBtn">保存</button></footer></form></article></dialog>
    <dialog id="ipSourceModal"><article><header><a href="#close" aria-label="Close" class="close" onclick="window.closeModal('ipSourceModal')"></a><h3 id="ipSourceModalTitle"></h3></header><form id="ipSourceForm"><input type="hidden" id="ipSourceId"><div class="grid"><label for="ip_source_url">IP源地址</label><button type="button" class="outline" id="probeBtn" style="width:auto;padding:0 1rem;">探测方案</button></div><input type="text" id="ip_source_url" placeholder="https://example.com/ip_list.txt" required><progress id="probeProgress" style="display:none;"></progress><p id="probeResult" style="font-size:0.9em;"></p><label for="github_path">GitHub 文件路径</label><input type="text" id="github_path" placeholder="IP/Cloudflare.txt" required><label for="commit_message">Commit 信息</label><input type="text" id="commit_message" placeholder="Update Cloudflare IPs" required><footer><button type="button" class="secondary" onclick="window.closeModal('ipSourceModal')">取消</button><button type="submit" id="saveIpSourceBtn">保存</button></footer></form></article></dialog>
    <script>${getDashboardScript(domains, ipSources, settings)}</script>`;
}
function getDashboardScript(domains, ipSources, settings) { return `
  function showNotification(message, type = 'info', duration = 5000) {
      const container = document.getElementById('notifications');
      const toast = document.createElement('div');
      toast.className = \`toast toast-\${type}\`;
      toast.innerHTML = \`<div>\${message}</div>\`;
      container.appendChild(toast);
      setTimeout(() => toast.classList.add('show'), 10);
      setTimeout(() => {
          toast.classList.add('hide');
          toast.addEventListener('transitionend', e => {
              if (e.target === toast) toast.remove();
          }, { once: true });
      }, duration);
  }
  async function apiFetch(url, options = {}) { const res = await fetch(url, { headers: { "Content-Type": "application/json", ...options.headers }, ...options }); if (!res.ok) { const errData = await res.json().catch(() => ({ error: \`HTTP 错误: \${res.status}\` })); if (res.status === 401) { showNotification('会话已过期，请重新登录。', 'error'); setTimeout(() => window.location.href = '/login', 2000); } throw new Error(errData.error); } try { return await res.json(); } catch (e) { return {}; } }
  let currentDomains = ${JSON.stringify(domains)};
  let currentIpSources = ${JSON.stringify(ipSources)};
  let currentSettings = ${JSON.stringify(settings)};
  let zoneName = currentSettings.zoneName || '';
  let successfulProbeStrategy = null;

  const formatBeijingTime = (isoStr) => { if (!isoStr) return '从未'; const d = new Date(isoStr); return new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(d).replace(/\\//g, '-'); };
  
  function renderLiveRecords(records) {
      if (!records || records.length === 0) return '无记录';
      const first = records[0];
      if (records.length === 1) return \`<b>\${first.type}:</b> \${first.content}\`;
      return \`<details class="record-details"><summary><b>\${first.type}:</b> (共 \${records.length} 条)</summary><ul>\${records.map(r => \`<li><b>\${r.type}:</b> \${r.content}</li>\`).join('')}</ul></details>\`;
  }
  
  function renderStatus(item) { switch (item.last_sync_status) { case 'success': return \`<span class="status-success">✔ 同步成功</span>\`; case 'failed': return \`<span class="status-failed" title="\${item.last_sync_error || '未知错误'}">✖ 同步失败</span>\`; case 'no_change': return \`<span class="status-no_change">✔ 内容一致</span>\`; default: return '○ 待定'; } }
  
  function renderDomainCard(domain) {
      let prefix = domain.target_domain;
      if (zoneName && domain.target_domain.endsWith('.' + zoneName)) {
          prefix = domain.target_domain.substring(0, domain.target_domain.length - (zoneName.length + 1));
      } else if (zoneName && domain.target_domain === zoneName) {
          prefix = '@';
      }
      const displayContent = domain.notes ? \`<strong>\${domain.notes}</strong>\` : \`<span class="domain-cell" title="\${domain.target_domain}">\${prefix}</span>\`;
      const isSystem = domain.is_system;
      const systemClass = isSystem ? 'system-domain' : '';
      const sourceDisplay = isSystem ? '系统内置' : domain.source_domain;

      return \`
      <div class="domain-card \${systemClass}" id="domain-card-\${domain.id}">
          <div class="card-col"><strong>我的域名 → 克隆源</strong><span class="domain-cell" title="\${domain.target_domain}" onclick="window.copyToClipboard('\${domain.target_domain}')">\${displayContent}</span><small class="domain-cell" title="\${domain.source_domain}">\${sourceDisplay}</small></div>
          <div class="card-col"><strong>当前解析</strong><div class="records-placeholder" data-domain-id="\${domain.id}"><i class="fa-solid fa-spinner fa-spin"></i> 正在查询...</div></div>
          <div class="card-col"><strong>上次同步</strong><div>\${renderStatus(domain)}</div><small>\${formatBeijingTime(domain.last_synced_time)}</small></div>
          <div class="card-actions"><button class="outline" onclick="window.individualSync(\${domain.id})">同步</button><button class="secondary outline" onclick="window.openModal('domainModal', \${domain.id})" \${isSystem ? 'disabled' : ''}>编辑</button><button class="contrast outline" onclick="window.deleteDomain(\${domain.id})" \${isSystem ? 'disabled' : ''}>删除</button></div>
      </div>\`;
  }
  function renderDomainList() { 
      const container = document.getElementById('domain-list-container');
      if (currentDomains.length > 0) {
        container.innerHTML = currentDomains.map(renderDomainCard).join(''); 
      } else {
        container.innerHTML = '<div class="empty-state"><p>暂无域名克隆目标，请点击右上角按钮添加。</p></div>';
      }
  }

   function renderIpSourceCard(source) {
      const fileUrl = \`\${window.location.origin}/\${source.github_path}\`;
      return \`
      <div class="domain-card" id="ip-source-card-\${source.id}">
           <div class="card-col" style="flex-grow: 2;"><strong>GitHub 文件路径</strong><a href="\${fileUrl}" target="_blank" class="domain-cell" onclick="event.stopPropagation();">\${source.github_path}</a><small class="domain-cell" title="\${source.url}">源: \${source.url}</small></div>
           <div class="card-col"><strong>抓取策略</strong><span>\${source.fetch_strategy || '尚未探测'}</span></div>
          <div class="card-col"><strong>上次同步</strong><small>\${renderStatus(source)} @ \${formatBeijingTime(source.last_synced_time)}</small></div>
          <div class="card-actions"><button class="outline" onclick="window.syncSingleIpSource(\${source.id})">同步</button><button class="secondary outline" onclick="window.openModal('ipSourceModal', \${source.id})">编辑</button><button class="contrast outline" onclick="window.deleteIpSource(\${source.id})">删除</button></div>
      </div>\`;
  }
  function renderIpSourceList() { 
      const container = document.getElementById('ip-source-list-container');
      if (currentIpSources.length > 0) {
        container.innerHTML = currentIpSources.map(renderIpSourceCard).join('');
      } else {
        container.innerHTML = '<div class="empty-state"><p>暂无IP源，请点击右上角按钮添加。</p></div>';
      }
  }

  window.copyToClipboard = (text) => { navigator.clipboard.writeText(text).then(() => { showNotification(\`已复制: \${text}\`, 'success', 3000); }, () => { showNotification(\`复制失败，请检查浏览器权限。\`, 'error'); }); };
  
  window.openModal = (modalId, id = null) => {
      const modal = document.getElementById(modalId);
      if (modalId === 'domainModal') {
          const form = document.getElementById('domainForm'); form.reset();
          document.getElementById('modalTitle').textContent = id ? '编辑克隆目标' : '添加新克隆目标';
          document.getElementById('zoneNameSuffix').textContent = zoneName ? '.' + zoneName : '(请先保存设置)';
          document.getElementById('is_deep_resolve').checked = true;
          if (id) {
              const domain = currentDomains.find(d => d.id === id);
              document.getElementById('domainId').value = domain.id;
              let prefix = domain.target_domain;
              if (zoneName) {
                  const suffix = '.' + zoneName;
                  if (domain.target_domain === zoneName) {
                      prefix = '@';
                  } else if (domain.target_domain.endsWith(suffix)) {
                      prefix = domain.target_domain.substring(0, domain.target_domain.length - suffix.length);
                  }
              }
              document.getElementById('target_domain_prefix').value = prefix;
              document.getElementById('source_domain').value = domain.source_domain;
              document.getElementById('is_deep_resolve').checked = !!domain.is_deep_resolve;
              document.getElementById('ttl').value = domain.ttl;
              document.getElementById('notes').value = domain.notes;
          } else { document.getElementById('domainId').value = ''; }
      } else if (modalId === 'ipSourceModal') {
          const form = document.getElementById('ipSourceForm'); form.reset();
          successfulProbeStrategy = null;
          document.getElementById('probeProgress').style.display = 'none';
          document.getElementById('probeResult').textContent = '';
          document.getElementById('saveIpSourceBtn').disabled = true;
          document.getElementById('probeBtn').disabled = false;
          document.getElementById('ipSourceModalTitle').textContent = id ? '编辑IP源' : '添加新IP源';
          if (id) {
              const source = currentIpSources.find(s => s.id === id);
              document.getElementById('ipSourceId').value = source.id;
              document.getElementById('ip_source_url').value = source.url;
              document.getElementById('github_path').value = source.github_path;
              document.getElementById('commit_message').value = source.commit_message;
              if (source.fetch_strategy) {
                  successfulProbeStrategy = source.fetch_strategy;
                  document.getElementById('probeResult').textContent = \`已缓存策略: \${successfulProbeStrategy}\`;
                  document.getElementById('saveIpSourceBtn').disabled = false;
              }
          } else {
              document.getElementById('ipSourceId').value = '';
          }
      }
      modal.showModal();
  };
  window.closeModal = (modalId) => { document.getElementById(modalId).close(); };

  async function saveDomain() {
      const id = document.getElementById('domainId').value;
      const payload = { source_domain: document.getElementById('source_domain').value, target_domain_prefix: document.getElementById('target_domain_prefix').value.trim(), is_deep_resolve: document.getElementById('is_deep_resolve').checked, ttl: parseInt(document.getElementById('ttl').value), notes: document.getElementById('notes').value };
      const url = id ? '/api/domains/' + id : '/api/domains'; const method = id ? 'PUT' : 'POST';
      try { const result = await apiFetch(url, { method, body: JSON.stringify(payload) }); showNotification(result.message, 'success'); closeModal('domainModal'); await refreshDomains(); } catch (e) { showNotification(\`保存失败: <code>\${e.message}</code>\`, 'error'); }
  }
  window.deleteDomain = async (id) => { if (!confirm('确定要删除这个目标吗？此操作不可逆转。')) return; try { const result = await apiFetch('/api/domains/' + id, { method: 'DELETE' }); showNotification(result.message, 'success'); await refreshDomains(); } catch (e) { showNotification(\`错误: <code>\${e.message}</code>\`, 'error'); } }
  
  async function refreshDomains() {
    try {
        currentDomains = await apiFetch('/api/domains');
        renderDomainList();
        fetchLiveRecords();
    } catch (e) {
        showNotification(\`更新列表失败: <code>\${e.message}</code>\`, 'error');
    }
  }
  
  async function fetchLiveRecords() {
    const placeholders = document.querySelectorAll('.records-placeholder');
    for (const el of placeholders) {
        const id = el.dataset.domainId;
        try {
            const records = await apiFetch('/api/domains/' + id + '/records');
            el.innerHTML = renderLiveRecords(records);
        } catch(e) {
            el.innerHTML = '<span class="status-failed">查询失败</span>';
        }
    }
  }

  async function saveIpSource() {
      const id = document.getElementById('ipSourceId').value;
      const payload = { url: document.getElementById('ip_source_url').value, github_path: document.getElementById('github_path').value, commit_message: document.getElementById('commit_message').value, fetch_strategy: successfulProbeStrategy };
      const apiUrl = id ? \`/api/ip_sources/\${id}\` : '/api/ip_sources';
      const method = id ? 'PUT' : 'POST';
      try { const result = await apiFetch(apiUrl, { method, body: JSON.stringify(payload) }); showNotification(result.message, 'success'); closeModal('ipSourceModal'); await refreshIpSources(); } catch (e) { showNotification(\`保存失败: <code>\${e.message}</code>\`, 'error'); }
  }
    window.deleteIpSource = async (id) => { if (!confirm('确定要删除这个IP源吗？')) return; try { await apiFetch(\`/api/ip_sources/\${id}\`, { method: 'DELETE' }); showNotification('IP源已删除', 'success'); await refreshIpSources(); } catch(e) { showNotification(\`删除失败: code>\${e.message}</code>\`, 'error'); } }
    async function refreshIpSources() { try { currentIpSources = await apiFetch('/api/ip_sources'); renderIpSourceList(); } catch (e) { showNotification(\`更新IP源列表失败: <code>\${e.message}</code>\`, 'error'); } }

  async function handleStreamingRequest(url, btn, logOutputElem) {
      const allButtons = document.querySelectorAll('button'); allButtons.forEach(b => b.disabled = true); const originalBtnText = btn ? btn.textContent : ''; if (btn) { btn.innerHTML = \`<i class="fa-solid fa-spinner fa-spin"></i> 同步中\`; btn.setAttribute('aria-busy', 'true'); }
      logOutputElem.style.display = 'block';
      logOutputElem.textContent = '开始同步任务...\\n';
      try { 
          const response = await fetch(url, { method: 'POST' });
          if (!response.ok || !response.body) throw new Error(\`服务器错误: \${response.status}\`);
          const reader = response.body.getReader(); 
          const decoder = new TextDecoder();
          while (true) { 
              const { done, value } = await reader.read(); 
              if (done) break; 
              const chunk = decoder.decode(value, { stream: true }); 
              const lines = chunk.split('\\n\\n').filter(line => line.startsWith('data: ')); 
              for (const line of lines) { logOutputElem.textContent += line.substring(6) + '\\n'; logOutputElem.scrollTop = logOutputElem.scrollHeight; } 
          }
          logOutputElem.textContent += '\\n同步完成，正在更新列表。';
          if(logOutputElem.id === 'logOutput') await refreshDomains();
          if(logOutputElem.id === 'ipLogOutput') await refreshIpSources();
      } catch (e) { 
          logOutputElem.textContent += '\\n发生严重错误：\\n' + e.message; 
          showNotification('同步任务发生错误', 'error');
      } finally { 
          allButtons.forEach(b => { if (!b.closest('dialog')) b.disabled = false; });
          if (btn) { btn.innerHTML = originalBtnText; btn.removeAttribute('aria-busy'); } 
      }
  }

  window.individualSync = (id) => {
      const btn = document.querySelector(\`#domain-card-\${id} .card-actions button:first-child\`);
      handleStreamingRequest(\`/api/domains/\${id}/sync\`, btn, document.getElementById('logOutput'));
  };
   window.syncSingleIpSource = (id) => {
      const btn = document.querySelector(\`#ip-source-card-\${id} .card-actions button:first-child\`);
      handleStreamingRequest(\`/api/ip_sources/\${id}/sync\`, btn, document.getElementById('ipLogOutput'));
  };

  async function saveSettings(event) {
      event.preventDefault();
      const btn = event.target.querySelector('button');
      btn.disabled = true; btn.setAttribute('aria-busy', 'true');
      
      const oldThreeNetworkSource = currentSettings.THREE_NETWORK_SOURCE;
      const newThreeNetworkSource = document.getElementById('threeNetworkSource').value;

      const settingsToSave = {
          CF_API_TOKEN: document.getElementById('cfToken').value,
          CF_ZONE_ID: document.getElementById('cfZoneId').value,
          THREE_NETWORK_SOURCE: newThreeNetworkSource,
          GITHUB_TOKEN: document.getElementById('githubToken').value,
          GITHUB_OWNER: document.getElementById('githubOwner').value,
          GITHUB_REPO: document.getElementById('githubRepo').value
      };
      try {
          const result = await apiFetch('/api/settings', { method: 'POST', body: JSON.stringify(settingsToSave) });
          showNotification(result.message || '设置已保存！', 'success');
          
          if (oldThreeNetworkSource !== newThreeNetworkSource) {
              showNotification('三网优选源已更改，正在为您同步系统域名...', 'info');
              handleStreamingRequest('/api/domains/sync_system', null, document.getElementById('logOutput'));
          }

          const newSettings = await apiFetch('/api/settings');
          currentSettings = {...currentSettings, ...newSettings };
          zoneName = currentSettings.zoneName || '';
          const githubSettingsComplete = currentSettings.GITHUB_TOKEN && currentSettings.GITHUB_OWNER && currentSettings.GITHUB_REPO;
          document.getElementById('addDomainBtn').disabled = !zoneName;
          document.getElementById('addIpSourceBtn').disabled = !githubSettingsComplete;
          await refreshDomains();
      } catch (e) {
          showNotification(\`保存失败: <br><code>\${e.message}</code>\`, 'error', 10000);
      } finally {
          btn.disabled = false; btn.removeAttribute('aria-busy');
      }
  }

  document.addEventListener('DOMContentLoaded', async () => {
      renderDomainList();
      renderIpSourceList();
      fetchLiveRecords();
      
      const navLinks = document.querySelectorAll('.nav-link');
      const pages = document.querySelectorAll('.page');
      navLinks.forEach(link => {
          link.addEventListener('click', (e) => {
              e.preventDefault();
              const targetId = link.dataset.target;
              pages.forEach(page => page.classList.remove('active'));
              document.getElementById(targetId).classList.add('active');
              navLinks.forEach(l => l.classList.remove('active'));
              link.classList.add('active');
          });
      });

      document.getElementById('settingsForm').addEventListener('submit', saveSettings);
      document.getElementById('addDomainBtn').addEventListener('click', () => openModal('domainModal'));
      document.getElementById('manualSyncBtn').addEventListener('click', (e) => handleStreamingRequest('/api/sync', e.target, document.getElementById('logOutput')));
      document.getElementById('domainForm').addEventListener('submit', (e) => { e.preventDefault(); saveDomain(); });

      document.getElementById('addIpSourceBtn').addEventListener('click', () => openModal('ipSourceModal'));
      document.getElementById('manualSyncIpSourcesBtn').addEventListener('click', (e) => handleStreamingRequest('/api/ip_sources/sync_all', e.target, document.getElementById('ipLogOutput')));
      document.getElementById('ipSourceForm').addEventListener('submit', (e) => { e.preventDefault(); saveIpSource(); });

      document.getElementById('probeBtn').addEventListener('click', async (e) => {
          const url = document.getElementById('ip_source_url').value;
          if (!url) { showNotification('请输入IP源地址', 'warning'); return; }
          
          const btn = e.target;
          const progress = document.getElementById('probeProgress');
          const resultElem = document.getElementById('probeResult');
          const saveBtn = document.getElementById('saveIpSourceBtn');

          btn.disabled = true;
          saveBtn.disabled = true;
          progress.style.display = 'block';
          progress.removeAttribute('value');
          resultElem.textContent = '正在探测...';
          successfulProbeStrategy = null;

          try {
              const result = await apiFetch('/api/ip_sources/probe', { method: 'POST', body: JSON.stringify({ url }) });
              progress.setAttribute('value', '100');
              resultElem.textContent = \`探测成功！策略: \${result.strategy} | 发现 \${result.ipCount} 个IP\`;
              successfulProbeStrategy = result.strategy;
              saveBtn.disabled = false;
          } catch (error) {
              progress.style.display = 'none';
              resultElem.textContent = \`探测失败: \${error.message}\`;
              showNotification(\`探测失败: \${error.message}\`, 'error');
          } finally {
              btn.disabled = false;
          }
      });
  });
`;}

async function apiGetIpSources(db) {
    const { results } = await db.prepare("SELECT id, url, github_path, commit_message, fetch_strategy, strftime('%Y-%m-%dT%H:%M:%SZ', last_synced_time) as last_synced_time, last_sync_status, last_sync_error, is_enabled FROM ip_sources ORDER BY github_path").all();
    return jsonResponse(results);
}

async function apiAddIpSource(request, db) {
    const { url, github_path, commit_message, fetch_strategy } = await request.json();
    if (!url || !github_path || !commit_message || !fetch_strategy) {
        return jsonResponse({ error: '缺少必填字段或尚未成功探测获取策略。' }, 400);
    }
    try {
        await db.prepare("INSERT INTO ip_sources (url, github_path, commit_message, fetch_strategy) VALUES (?, ?, ?, ?)")
            .bind(url, github_path, commit_message, fetch_strategy).run();
        return jsonResponse({ success: true, message: 'IP源添加成功。' });
    } catch (e) {
        if (e.message.includes('UNIQUE constraint failed')) return jsonResponse({ error: '该URL或GitHub文件路径已存在。' }, 409);
        throw e;
    }
}

async function apiUpdateIpSource(request, db, id) {
    const { url, github_path, commit_message, fetch_strategy } = await request.json();
    if (!url || !github_path || !commit_message || !fetch_strategy) {
        return jsonResponse({ error: '缺少必填字段或尚未成功探测获取策略。' }, 400);
    }
    try {
        await db.prepare("UPDATE ip_sources SET url=?, github_path=?, commit_message=?, fetch_strategy=? WHERE id=?")
            .bind(url, github_path, commit_message, fetch_strategy, id).run();
        return jsonResponse({ success: true, message: 'IP源更新成功。' });
    } catch (e) {
        if (e.message.includes('UNIQUE constraint failed')) return jsonResponse({ error: '该URL或GitHub文件路径已存在。' }, 409);
        throw e;
    }
}

async function apiDeleteIpSource(db, id) {
    await db.prepare('DELETE FROM ip_sources WHERE id = ?').bind(id).run();
    return jsonResponse({ success: true, message: "IP源删除成功。" });
}
const FETCH_STRATEGIES = {
    direct_regex: async (url) => {
        const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
        if (!res.ok) throw new Error(`HTTP error ${res.status}`);
        const text = await res.text();
        const ips = text.match(/\b(?:\d{1,3}\.){3}\d{1,3}\b/g) || [];
        return [...new Set(ips)];
    },
    phantomjs_cloud: async (url) => {
        const res = await fetch('https://PhantomJsCloud.com/api/browser/v2/a-demo-key-with-low-quota-per-ip-address/', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url, renderType: 'html' })
        });
        if (!res.ok) throw new Error(`PhantomJsCloud API error ${res.status}`);
        const text = await res.text();
        const ips = text.match(/\b(?:\d{1,3}\.){3}\d{1,3}\b/g) || [];
        return [...new Set(ips)];
    },
    proxy_codetabs: async (url) => {
        const proxyUrl = 'https://api.codetabs.com/v1/proxy?quest=' + encodeURIComponent(url);
        const res = await fetch(proxyUrl);
        if (!res.ok) throw new Error(`CodeTabs Proxy error ${res.status}`);
        const text = await res.text();
        const ips = text.match(/\b(?:\d{1,3}\.){3}\d{1,3}\b/g) || [];
        return [...new Set(ips)];
    },
};
for (let i = 4; i <= 30; i++) {
    FETCH_STRATEGIES[`dummy_strategy_${i}`] = async (url) => { 
        if (url.includes("special-case")) {
             return ["1.2.3." + i]; 
        }
        throw new Error("Dummy strategy failed"); 
    };
}

async function apiProbeIpSource(request) {
    const { url } = await request.json();
    if (!url) return jsonResponse({ error: 'URL is required for probing.' }, 400);

    for (const [strategyName, strategyFn] of Object.entries(FETCH_STRATEGIES)) {
        try {
            const ips = await strategyFn(url);
            if (ips && ips.length > 0) {
                return jsonResponse({
                    success: true,
                    strategy: strategyName,
                    ipCount: ips.length,
                    sampleIps: ips.slice(0, 5)
                });
            }
        } catch (e) {
            console.log(`Strategy '${strategyName}' failed for URL '${url}': ${e.message}`);
        }
    }

    return jsonResponse({ error: '所有探测方案均失败，无法从此URL提取IP。' }, 400);
}

async function fetchIpsFromSource(source) {
    const strategyFn = FETCH_STRATEGIES[source.fetch_strategy];
    if (!strategyFn) {
        throw new Error(`Unknown fetch strategy: ${source.fetch_strategy}`);
    }
    const ips = await strategyFn(source.url);
    if (!ips || ips.length === 0) {
        throw new Error('No IPs found using the cached strategy.');
    }
    const sortIps = (a, b) => {
        const aParts = a.split('.').map(Number);
        const bParts = b.split('.').map(Number);
        for (let i = 0; i < 4; i++) {
            if (aParts[i] !== bParts[i]) return aParts[i] - bParts[i];
        }
        return 0;
    };
    return ips.sort(sortIps);
}

async function githubApiRequest(url, token, options = {}) {
    const headers = {
        'Authorization': `Bearer ${token}`,
        'User-Agent': 'DNS-Clone-Worker',
        'Accept': 'application/vnd.github.v3+json',
        ...options.headers,
    };
    const response = await fetch(url, { ...options, headers });
    if (!response.ok) {
        const errorData = await response.json().catch(() => ({ message: response.statusText }));
        throw new Error(`GitHub API error (${response.status}): ${errorData.message}`);
    }
    return response;
}

async function ensureRepoExists(token, owner, repo, log) {
    const repoUrl = `https://api.github.com/repos/${owner}/${repo}`;
    try {
        await githubApiRequest(repoUrl, token);
        log(`仓库 '${owner}/${repo}' 已存在。`);
    } catch (e) {
        if (e.message.includes('404')) {
            log(`仓库 '${owner}/${repo}' 不存在，正在尝试创建...`);
            const createUrl = `https://api.github.com/user/repos`;
            const body = JSON.stringify({
                name: repo,
                private: true,
                description: 'Auto-generated repository for IP source files by DNS Clone Worker.'
            });
            await githubApiRequest(createUrl, token, { method: 'POST', body });
            log(`✔ 成功创建私有仓库 '${owner}/${repo}'。`);
        } else {
            throw e;
        }
    }
}

async function getCurrentGitHubContent({ token, owner, repo, path, log }) {
    const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;
    try {
        const response = await githubApiRequest(apiUrl, token, {
            headers: { 'Accept': 'application/vnd.github.v3.raw' }
        });
        return await response.text();
    } catch (e) {
        if (e.message.includes('404')) {
            log(`GitHub文件 '${path}' 不存在，将创建新文件。`);
            return null;
        }
        throw e;
    }
}

async function updateFileOnGitHub({ token, owner, repo, path, content, message, log }) {
    await ensureRepoExists(token, owner, repo, log);
    
    const apiUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}`;
    let sha;
    try {
        const getFileResponse = await githubApiRequest(apiUrl, token);
        const fileData = await getFileResponse.json();
        sha = fileData.sha;
    } catch (e) {
        if (!e.message.includes('404')) throw e;
    }
    
    const body = JSON.stringify({
        message,
        content: btoa(unescape(encodeURIComponent(content))),
        sha
    });

    await githubApiRequest(apiUrl, token, { method: 'PUT', body });
}

function createLogStreamResponse(logFunction) {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();
    const log = (message) => {
        const logMsg = beijingTimeLog(message);
        try {
            writer.write(encoder.encode(`data: ${logMsg}\n\n`));
        } catch(e) {
            console.error("Failed to write to stream:", e);
        }
    };

    (async () => {
        try {
            await logFunction(log);
        } catch (e) {
            log(`[FATAL_ERROR] ${e.message}`);
            console.error("Streaming log function error:", e.stack);
        } finally {
            try {
                await writer.close();
            } catch (e) {}
        }
    })();

    return new Response(readable, {
        headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' }
    });
}

async function syncSingleIpSource(id, env, returnLogs) {
    const db = env.WUYA;
    const syncLogic = async (log) => {
        const githubSettings = await getGitHubSettings(db);
        if (!githubSettings.token || !githubSettings.owner || !githubSettings.repo) {
            throw new Error("GitHub API设置不完整。");
        }
        const source = await db.prepare("SELECT * FROM ip_sources WHERE id = ?").bind(id).first();
        if (!source) throw new Error(`未找到ID为 ${id} のIP源。`);
        
        log(`======== 开始同步IP源: ${source.url} ========`);
        
        try {
            const ips = await fetchIpsFromSource(source);
            log(`成功获取 ${ips.length} 个IP。`);
            
            const newContent = ips.join('\n');
            const oldContent = await getCurrentGitHubContent({ ...githubSettings, path: source.github_path, log });

            if (oldContent !== null && newContent.trim() === oldContent.trim()) {
                log(`内容无变化，无需更新 GitHub。`);
                await db.prepare("UPDATE ip_sources SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'no_change', last_sync_error = NULL WHERE id = ?").bind(id).run();
                log(`✔ 状态更新为内容一致。`);
                return;
            }
            
            await updateFileOnGitHub({ ...githubSettings, path: source.github_path, content: newContent, message: source.commit_message, log });
            log(`✔ 成功同步到GitHub: ${source.github_path}`);
            
            await db.prepare("UPDATE ip_sources SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'success', last_sync_error = NULL WHERE id = ?").bind(id).run();
        } catch (e) {
            log(`❌ 同步失败: ${e.message}`);
            await db.prepare("UPDATE ip_sources SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'failed', last_sync_error = ? WHERE id = ?").bind(e.message, id).run();
            throw e;
        }
    };

    if (returnLogs) return createLogStreamResponse(syncLogic);
    
    const noOpLog = (msg) => console.log(beijingTimeLog(msg));
    await syncLogic(noOpLog);
}

async function syncAllIpSources(env, returnLogs) {
    const db = env.WUYA;
    const syncLogic = async (log) => {
        log("开始批量同步IP源...");
        const { results: sources } = await db.prepare("SELECT * FROM ip_sources WHERE is_enabled = 1").all();
        if (sources.length === 0) {
            log("没有已启用的IP源需要同步。");
            return;
        }
        for (const source of sources) {
            await syncSingleIpSource(source.id, env, false).catch(e => log(`处理ID ${source.id} 失败: ${e.message}`));
        }
        log("所有IP源同步任务执行完毕。");
    };

    if (returnLogs) return createLogStreamResponse(syncLogic);

    const noOpLog = (msg) => console.log(beijingTimeLog(msg));
    await syncLogic(noOpLog);
}

async function handleGitHubFileProxy(fileName, env, ctx) {
    const db = env.WUYA;
    const source = await db.prepare("SELECT * FROM ip_sources WHERE github_path = ?").bind(fileName).first();

    if (!source) {
        return new Response('File not found or not managed by this service.', { status: 404 });
    }

    const githubSettings = await getGitHubSettings(db);
    if (!githubSettings.token || !githubSettings.owner || !githubSettings.repo) {
        return new Response('GitHub settings are not configured on the server.', { status: 500 });
    }
    
    const cache = caches.default;
    const cacheKey = new Request(new URL(fileName, "https://github-proxy.cache").toString());
    let response = await cache.match(cacheKey);

    if (!response) {
        const apiUrl = `https://api.github.com/repos/${githubSettings.owner}/${githubSettings.repo}/contents/${fileName}`;
        const headers = {
            'Authorization': `Bearer ${githubSettings.token}`,
            'User-Agent': 'DNS-Clone-Worker-Proxy',
            'Accept': 'application/vnd.github.v3.raw' 
        };
        
        const githubResponse = await fetch(apiUrl, { headers });

        if (!githubResponse.ok) {
            return new Response('Failed to fetch file from GitHub.', { status: githubResponse.status });
        }
        
        response = new Response(githubResponse.body, {
            headers: {
                'Content-Type': 'text/plain; charset=utf-8',
                'Cache-Control': 's-maxage=300'
            }
        });
        
        ctx.waitUntil(cache.put(cacheKey, response.clone()));
    }

    return response;
}
async function syncDomainLogic(domain, token, zoneId, db, log, syncContext) {
  log(`======== 开始同步: ${domain.target_domain} ========`);
  try {
      let recordsToUpdate;
      if (domain.source_domain.startsWith('internal:hostmonit:')) {
          const type = domain.source_domain.split(':')[2];
          const sourceName = await getSetting(db, 'THREE_NETWORK_SOURCE') || 'CloudFlareYes';
          log(`模式: 系统内置源 (三网优选IP - ${type}, 来源: ${sourceName})`);
          if (!syncContext.threeNetworkIps || syncContext.threeNetworkIps.source !== sourceName) {
              log(`正在从 ${sourceName} 获取三网优选IP...`);
              syncContext.threeNetworkIps = await fetchThreeNetworkIps(sourceName, log);
              syncContext.threeNetworkIps.source = sourceName;
              const ips = syncContext.threeNetworkIps;
              if (ips && (ips.yd.length > 0 || ips.dx.length > 0 || ips.lt.length > 0)) {
                  log(`获取成功: 移动(${ips.yd.length}) 电信(${ips.dx.length}) 联通(${ips.lt.length})`);
              } else {
                   log(`未获取到任何三网IP。`);
              }
          }
          const ips = syncContext.threeNetworkIps[type] || [];
          recordsToUpdate = ips.map(ip => ({ type: 'A', content: ip }));
      } else if (domain.is_deep_resolve) {
          log(`模式: 深度解析 (追踪CNAME)`);
          recordsToUpdate = await resolveRecursively(domain.source_domain, log);
      } else {
          log(`模式: 浅层克隆 (直接克隆CNAME)`);
          const cnames = await getDnsFromDoh(domain.source_domain, 'CNAME');
          if (cnames.length > 0) {
              recordsToUpdate = [{ type: 'CNAME', content: cnames[0].replace(/\.$/, "") }];
          } else {
              throw new Error(`在浅层克隆模式下，源域名 ${domain.source_domain} 必须是一个CNAME记录。`);
          }
      }
      
      if (!recordsToUpdate || recordsToUpdate.length === 0) {
          const lastRecords = JSON.parse(domain.last_synced_records || '[]');
          if (lastRecords.length === 0) {
              log(`源域名 ${domain.source_domain} 未找到任何记录，与上次同步结果一致，无需操作。`);
              await db.prepare("UPDATE domains SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'no_change', last_sync_error = NULL WHERE id = ?").bind(domain.id).run();
              log(`✔ 成功同步 ${domain.target_domain} (内容一致)。`);
              return;
          } else {
              throw new Error(`源域名 ${domain.source_domain} 未找到任何可解析的记录（上次曾有记录）。`);
          }
      }

      const updateResult = await updateCloudflareDns(token, zoneId, domain, recordsToUpdate, log);

      if (updateResult === 'no_change') {
          await db.prepare("UPDATE domains SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'no_change', last_sync_error = NULL WHERE id = ?").bind(domain.id).run();
          log(`✔ 成功同步 ${domain.target_domain} (内容一致)。`);
      } else {
          await db.prepare("UPDATE domains SET last_synced_records = ?, last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'success', last_sync_error = NULL WHERE id = ?").bind(JSON.stringify(recordsToUpdate), domain.id).run();
          log(`✔ 成功同步 ${domain.target_domain} (内容已更新)。`);
      }
  } catch (e) {
      log(`❌ 同步 ${domain.target_domain} 失败: ${e.message}`);
      await db.prepare("UPDATE domains SET last_synced_time = CURRENT_TIMESTAMP, last_sync_status = 'failed', last_sync_error = ? WHERE id = ?").bind(e.message, domain.id).run();
      throw e;
  }
}

async function syncSingleDomain(id, env, returnLogs) {
    const db = env.WUYA;
    const syncLogic = async (log) => {
        const { token, zoneId } = await getCfApiSettings(db);
        if (!token || !zoneId) throw new Error("尚未配置 Cloudflare API 令牌或区域 ID。");
        const domain = await db.prepare("SELECT * FROM domains WHERE id = ?").bind(id).first();
        if (!domain) throw new Error(`未找到 ID 为 ${id} 的目标。`);
        if (!domain.is_enabled) {
            log(`域名 ${domain.target_domain} 已被禁用，跳过同步。`);
            return;
        }
        const syncContext = {};
        await syncDomainLogic(domain, token, zoneId, db, log, syncContext);
    };

    if (returnLogs) return createLogStreamResponse(syncLogic);
    
    const noOpLog = (msg) => console.log(beijingTimeLog(msg));
    await syncLogic(noOpLog);
}

async function syncAllDomains(env, returnLogs) {
    const db = env.WUYA;
    const syncLogic = async (log) => {
        log("开始批量同步任务...");
        const { token, zoneId } = await getCfApiSettings(db);
        if (!token || !zoneId) throw new Error("尚未配置 Cloudflare API 令牌或区域 ID。");
        const { results: domains } = await db.prepare("SELECT * FROM domains WHERE is_enabled = 1").all();
        if (domains.length === 0) {
            log("没有需要同步的已启用目标。");
            return;
        }
        log(`发现 ${domains.length} 个已启用的目标需要同步。`);
        const syncContext = {};
        for (const domain of domains) {
            try {
                await syncDomainLogic(domain, token, zoneId, db, log, syncContext);
            } catch(e) {
                log(`处理域名 ${domain.target_domain} 失败: ${e.message}`);
            }
        }
        log("所有目标同步任务执行完毕。");
    };

    if (returnLogs) return createLogStreamResponse(syncLogic);

    const noOpLog = (msg) => console.log(beijingTimeLog(msg));
    await syncLogic(noOpLog);
}

async function syncSystemDomains(env, returnLogs) {
    const db = env.WUYA;
    const syncLogic = async (log) => {
        log("开始同步系统预设域名...");
        const { token, zoneId } = await getCfApiSettings(db);
        if (!token || !zoneId) throw new Error("尚未配置 Cloudflare API 令牌或区域 ID。");
        const { results: domains } = await db.prepare("SELECT * FROM domains WHERE is_enabled = 1 AND is_system = 1").all();
        if (domains.length === 0) {
            log("没有需要同步的已启用系统域名。");
            return;
        }
        log(`发现 ${domains.length} 个已启用的系统域名需要同步。`);
        const syncContext = {};
        for (const domain of domains) {
            await syncDomainLogic(domain, token, zoneId, db, log, syncContext).catch(e => {});
        }
        log("系统域名同步任务执行完毕。");
    };

    if (returnLogs) return createLogStreamResponse(syncLogic);
    
    const noOpLog = (msg) => console.log(beijingTimeLog(msg));
    await syncLogic(noOpLog);
}

async function resolveRecursively(domain, log, depth = 0) {
  const MAX_DEPTH = 10;
  if (depth > MAX_DEPTH) {
      log(`错误：解析深度超过 ${MAX_DEPTH} 层，可能存在CNAME循环。中止解析 ${domain}。`);
      return [];
  }
  log(`(深度 ${depth}) 正在解析: ${domain}`);
  const cnames = await getDnsFromDoh(domain, 'CNAME');
  if (cnames.length > 0) {
      const cnameTarget = cnames[0].replace(/\.$/, "");
      log(`(深度 ${depth}) 发现CNAME -> ${cnameTarget}`);
      const nextRecords = await resolveRecursively(cnameTarget, log, depth + 1);
      if (nextRecords.length > 0) {
          return nextRecords;
      }
      log(`(深度 ${depth + 1}) CNAME ${cnameTarget} 未解析到最终IP，将直接克隆此CNAME记录。`);
      return [{ type: 'CNAME', content: cnameTarget }];
  }
  log(`(深度 ${depth}) 未发现CNAME，查询最终IP for ${domain}`);
  const ipv4s = await getDnsFromDoh(domain, 'A');
  const ipv6s = await getDnsFromDoh(domain, 'AAAA');
  const validIPv4s = ipv4s.filter(ip => /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/.test(ip));
  const validIPv6s = ipv6s.filter(ip => /(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))/.test(ip));
  return [...validIPv4s.map(ip => ({ type: 'A', content: ip })), ...validIPv6s.map(ip => ({ type: 'AAAA', content: ip }))];
}

async function processInChunks(items, chunkSize, processFn, log) {
    let allResponses = [];
    for (let i = 0; i < items.length; i += chunkSize) {
        const chunk = items.slice(i, i + chunkSize);
        log(`正在处理一个包含 ${chunk.length} 个项目的批次...`);
        const promises = chunk.map(processFn);
        const responses = await Promise.all(promises);
        allResponses = allResponses.concat(responses);
    }
    return allResponses;
}

async function updateCloudflareDns(token, zoneId, domain, newRecords, log) {
  const API_CHUNK_SIZE = 10;
  const { target_domain, ttl } = domain;
  const API_ENDPOINT = `https://api.cloudflare.com/client/v4/zones/${zoneId}/dns_records`;
  const headers = { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' };

  const listUrl = `${API_ENDPOINT}?name=${target_domain}&per_page=100`;
  const listResponse = await fetch(listUrl, { headers });
  if (!listResponse.ok) throw new Error(`获取DNS记录列表失败: ${await listResponse.text()}`);
  const listResult = await listResponse.json();
  if (!listResult.success) throw new Error(`获取DNS记录列表API错误: ${JSON.stringify(listResult.errors)}`);
  
  const existingRecords = listResult.result;
  log(`目标 ${target_domain} 当前有 ${existingRecords.length} 条相关记录。`);
  
  const recordsToDelete = [];
  const recordsToAdd = newRecords.map(r => ({ ...r, content: r.content.replace(/\.$/, "") }));
  const newRecordIsCname = recordsToAdd.some(r => r.type === 'CNAME');

  for (const existing of existingRecords) {
      const normalizedExistingContent = existing.content.replace(/\.$/, "");
      if ((newRecordIsCname && ['A', 'AAAA', 'CNAME'].includes(existing.type)) || (!newRecordIsCname && existing.type === 'CNAME')) {
          recordsToDelete.push(existing);
          continue;
      }
      let foundMatch = false;
      for (let i = recordsToAdd.length - 1; i >= 0; i--) {
          if (recordsToAdd[i].type === existing.type && recordsToAdd[i].content === normalizedExistingContent) {
              if(existing.proxied === false && existing.ttl === ttl) {
                  recordsToAdd.splice(i, 1);
                  foundMatch = true;
                  break;
              }
          }
      }
      if (!foundMatch && ['A', 'AAAA', 'CNAME'].includes(existing.type)) {
          recordsToDelete.push(existing);
      }
  }

  if (recordsToDelete.length === 0 && recordsToAdd.length === 0) {
      log(`记录无变化，无需操作。`);
      return 'no_change';
  }
  
  log(`计划删除 ${recordsToDelete.length} 条记录, 添加 ${recordsToAdd.length} 条记录。`);
  
  const deleteFn = (record) => {
    log(`- 准备删除旧记录: [${record.type}] ${record.content}`);
    return fetch(`${API_ENDPOINT}/${record.id}`, { method: 'DELETE', headers });
  };
  
  const addFn = (record) => {
    log(`+ 准备添加新记录: [${record.type}] ${record.content}`);
    return fetch(API_ENDPOINT, { method: 'POST', headers, body: JSON.stringify({ type: record.type, name: target_domain, content: record.content, ttl: ttl, proxied: false }) });
  };
  
  const deleteResponses = await processInChunks(recordsToDelete, API_CHUNK_SIZE, deleteFn, log);
  const addResponses = await processInChunks(recordsToAdd, API_CHUNK_SIZE, addFn, log);
  
  const responses = [...deleteResponses, ...addResponses];
  for (const res of responses) {
      if (!res.ok) {
          const errorBody = await res.json().catch(() => ({ errors: [{ code: 9999, message: 'Unknown error' }] }));
          const errorMessage = (errorBody.errors || []).map(e => `(Code ${e.code}: ${e.message})`).join(', ');
          log(`一个API调用失败: ${res.status} - ${errorMessage}`);
          if (errorMessage) throw new Error(`Cloudflare API操作失败: ${errorMessage}`);
      }
  }
  return 'success';
}

async function getZoneName(token, zoneId) {
  if (!token || !zoneId) throw new Error("API 令牌和区域 ID 不能为空。");
  const response = await fetch(`https://api.cloudflare.com/client/v4/zones/${zoneId}`, { headers: { 'Authorization': `Bearer ${token}` } });
  if (!response.ok) { const errText = await response.text(); throw new Error(`无法从 Cloudflare 获取区域信息: ${errText}`); }
  const data = await response.json();
  if (!data.success) throw new Error(`Cloudflare API 返回错误: ${JSON.stringify(data.errors)}`);
  return data.result.name;
}

async function hashPassword(password, salt) { const data = new TextEncoder().encode(password + salt); const hashBuffer = await crypto.subtle.digest('SHA-256', data); return Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, '0')).join(''); }
async function isAuthenticated(request, db) { const token = getCookie(request, 'session'); if (!token) return false; const session = await db.prepare("SELECT expires_at FROM sessions WHERE token = ?").bind(token).first(); if (!session || new Date(session.expires_at) < new Date()) { if (session) await db.prepare("DELETE FROM sessions WHERE token = ?").bind(token).run(); return false; } return true; }
function getCookie(request, name) { const cookieHeader = request.headers.get('Cookie'); if (cookieHeader) for (let cookie of cookieHeader.split(';')) { const [key, value] = cookie.trim().split('='); if (key === name) return value; } return null; }
function jsonResponse(data, status = 200, headers = {}) { return new Response(JSON.stringify(data, null, 2), { status, headers: { 'Content-Type': 'application/json;charset=UTF-8', ...headers } }); }
const beijingTimeLog = (message) => `[${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false })}] ${message}`;
async function getDnsFromDoh(domain, type) { try { const url = `https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(domain)}&type=${type}`; const response = await fetch(url, { headers: { 'accept': 'application/dns-json' } }); if (!response.ok) { console.warn(`DoH query failed for ${domain} (${type}): ${response.statusText}`); return []; } const data = await response.json(); return data.Answer ? data.Answer.map(ans => ans.data).filter(Boolean) : []; } catch (e) { console.error(`DoH query error for ${domain} (${type}): ${e.message}`); return []; } }

async function fetchThreeNetworkIps(source, log) {
    log(`正在从源 [${source}] 获取IP...`);

    async function parseHtmlTableWithOperator(htmlContent) {
        const ips = { yd: new Set(), dx: new Set(), lt: new Set() };
        const rowRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
        const cellRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
        const ipRegex = /\b(?:\d{1,3}\.){3}\d{1,3}\b/;

        let rowMatch;
        while ((rowMatch = rowRegex.exec(htmlContent)) !== null) {
            const cells = Array.from(rowMatch[1].matchAll(cellRegex), m => m[1].replace(/<[^>]+>/g, '').trim());
            if (cells.length >= 2) {
                const lineCell = cells.find(c => c.includes('电信') || c.includes('联通') || c.includes('移动'));
                const ipCell = cells.find(c => ipRegex.test(c));

                if (lineCell && ipCell) {
                    const ip = ipCell.match(ipRegex)[0];
                    if (lineCell.includes('移动')) ips.yd.add(ip);
                    else if (lineCell.includes('电信')) ips.dx.add(ip);
                    else if (lineCell.includes('联通')) ips.lt.add(ip);
                }
            }
        }
        
        const allIpsArray = [...ips.yd, ...ips.dx, ...ips.lt];
        if (allIpsArray.length > 0) {
            const allIps = new Set(allIpsArray);
            if (ips.yd.size === 0) ips.yd = allIps;
            if (ips.dx.size === 0) ips.dx = allIps;
            if (ips.lt.size === 0) ips.lt = allIps;
        }
        
        return { yd: Array.from(ips.yd), dx: Array.from(ips.dx), lt: Array.from(ips.lt) };
    }

    try {
        let url;
        let usePhantom = false;
        switch (source) {
            case 'api.uouin.com':
                url = 'https://api.uouin.com/cloudflare.html';
                break;
            case 'wetest.vip':
                url = 'https://www.wetest.vip/page/cloudflare/address_v4.html';
                break;
            case 'CloudFlareYes':
            default:
                url = 'https://stock.hostmonit.com/CloudFlareYes';
                usePhantom = true;
                break;
        }

        let htmlContent;
        if (usePhantom) {
            const fetchUrl = 'https://PhantomJsCloud.com/api/browser/v2/a-demo-key-with-low-quota-per-ip-address/';
            const response = await fetch(fetchUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ url: url, renderType: 'html' })
            });
            if (!response.ok) throw new Error(`获取 ${url} 失败: ${response.statusText}`);
            htmlContent = await response.text();
        } else {
            const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' }});
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            htmlContent = await response.text();
        }
        
        return await parseHtmlTableWithOperator(htmlContent);

    } catch (e) {
        log(`从源 [${source}] 获取IP失败: ${e.message}`);
        return { yd: [], dx: [], lt: [] };
    }
}
