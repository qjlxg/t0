const fs = require('fs');
const path = require('path');

// --- 配置项 ---
const CONCURRENCY = 30;         // 并发数
const MIN_VOLUME = 2000;        // 最低成交额门槛：1000万人民币（低于此数不预警、不自动记录）
const PREMIUM_THRESHOLD = -1.3; // 折价预警阈值：低于 -1.5% 触发
const DATA_DIR = path.join(process.cwd(), 'data');
const PORTFOLIO_PATH = path.join(DATA_DIR, 'portfolio.json');

// 确保数据目录存在
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

/**
 * 获取单个 ETF 的行情和估算净值
 */
async function fetchETF(code) {
    try {
        const market = (code.startsWith('5') || code.startsWith('6')) ? '1' : '0';
        // f43: 价格, f170: 涨跌幅, f47: 成交额, f58: 名称
        const quoteUrl = `https://push2.eastmoney.com/api/qt/stock/get?secid=${market}.${code}&fields=f43,f170,f47,f58`;
        const navUrl = `https://fundgz.1234567.com.cn/js/${code}.js?rt=${Date.now()}`;

        const [qRes, navText] = await Promise.all([
            fetch(quoteUrl).then(r => r.json()),
            fetch(navUrl).then(r => r.text()).catch(() => '')
        ]);

        if (!qRes || !qRes.data) return null;

        const navMatch = navText.match(/jsonpgz\((.+)\)/);
        const navData = navMatch ? JSON.parse(navMatch[1]) : null;
        
        const price = qRes.data.f43 / 1000;
        const nav = navData ? parseFloat(navData.dwjz) : 0;
        const volume = qRes.data.f47 / 10000; // 单位：万元
        
        if (nav <= 0) return null;

        const premiumVal = ((price - nav) / nav * 100);
        
        return {
            code,
            name: qRes.data.f58,
            price: price, 
            nav: nav.toFixed(4),
            premium: premiumVal.toFixed(2) + '%',
            premiumRaw: premiumVal,
            change: (qRes.data.f170 / 100).toFixed(2) + '%',
            vol: volume.toFixed(2),
            volRaw: volume
        };
    } catch (e) {
        return null;
    }
}

async function run() {
    console.log(`🚀 开始扫描 ETF (门槛: 折价 < ${PREMIUM_THRESHOLD}%, 成交额 > ${MIN_VOLUME}万)`);
    
    const etfPath = path.join(process.cwd(), 'etf.txt');
    if (!fs.existsSync(etfPath)) {
        console.error('❌ 错误: 未找到 etf.txt 文件');
        return;
    }

    const codes = fs.readFileSync(etfPath, 'utf-8')
        .split(/\r?\n/)
        .map(l => l.trim())
        .filter(l => /^\d{6}$/.test(l));
    
    // 分批抓取数据
    const allResults = [];
    for (let i = 0; i < codes.length; i += CONCURRENCY) {
        const batch = await Promise.all(codes.slice(i, i + CONCURRENCY).map(fetchETF));
        allResults.push(...batch.filter(Boolean));
        console.log(`📦 进度: ${Math.min(i + CONCURRENCY, codes.length)}/${codes.length}`);
    }

    const timestamp = new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });

    // --- 逻辑 1: 更新持仓账本 (Portfolio) ---
    let portfolio = {};
    if (fs.existsSync(PORTFOLIO_PATH)) {
        try {
            portfolio = JSON.parse(fs.readFileSync(PORTFOLIO_PATH, 'utf-8'));
        } catch (e) { portfolio = {}; }
    }

    // 只有成交额达标 且 折价够深，才自动加入账本
    allResults.forEach(r => {
        if (r.premiumRaw < PREMIUM_THRESHOLD && r.volRaw >= MIN_VOLUME && !portfolio[r.code]) {
            portfolio[r.code] = {
                name: r.name,
                buyPrice: r.price,
                buyDate: timestamp.split(' ')[0],
                status: '高折价买入(机会)'
            };
            console.log(`✨ 发现新机会并记录: ${r.name} (${r.code})`);
        }
    });
    fs.writeFileSync(PORTFOLIO_PATH, JSON.stringify(portfolio, null, 2));

    // --- 逻辑 2: 生成盈亏追踪表 (Profit Tracker) ---
    const profitHeader = '\ufeff代码,名称,买入日期,买入价格,当前价格,盈亏比例,当前折价,更新时间\n';
    const profitRows = Object.keys(portfolio).map(code => {
        const current = allResults.find(r => r.code === code);
        const p = portfolio[code];
        if (!current) return null;
        const profitPct = ((current.price - p.buyPrice) / p.buyPrice * 100).toFixed(2) + '%';
        return `${code},${p.name},${p.buyDate},${p.buyPrice},${current.price.toFixed(3)},${profitPct},${current.premium},${timestamp}`;
    }).filter(Boolean).join('\n');
    fs.writeFileSync(path.join(DATA_DIR, 'profit_tracker.csv'), profitHeader + profitRows);

    // --- 逻辑 3: 生成全量有效数据表 (All Data) ---
    const allHeader = '\ufeff代码,名称,价格,估算净值,溢价率,涨跌幅,成交额(万),更新时间\n';
    const allRows = allResults.map(r => 
        `${r.code},${r.name},${r.price.toFixed(3)},${r.nav},${r.premium},${r.change},${r.vol},${timestamp}`
    ).join('\n');
    fs.writeFileSync(path.join(DATA_DIR, 'all_valid_data.csv'), allHeader + allRows);

    // --- 逻辑 4: 生成折价预警报告 (仅含高流动性品种) ---
    const alertHeader = '\ufeff代码,名称,价格,溢价率,成交额(万),涨跌幅,更新时间\n';
    const alertRows = allResults
        .filter(r => r.premiumRaw < PREMIUM_THRESHOLD && r.volRaw >= MIN_VOLUME)
        .sort((a, b) => a.premiumRaw - b.premiumRaw) // 按折价程度排序，最便宜的在前
        .map(r => `${r.code},${r.name},${r.price.toFixed(3)},${r.premium},${r.vol},${r.change},${timestamp}`)
        .join('\n');
    fs.writeFileSync(path.join(DATA_DIR, 'high_premium_alert.csv'), alertHeader + alertRows);

    console.log(`\n✅ 任务执行完毕！`);
    console.log(`- 活跃折价标的: ${alertRows ? alertRows.split('\n').length : 0} 个`);
    console.log(`- 正在追踪持仓: ${Object.keys(portfolio).length} 个`);
}

run();
