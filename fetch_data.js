const fs = require('fs');
const path = require('path');

const CONCURRENCY = 30; 
const DATA_DIR = path.join(process.cwd(), 'data');
const PORTFOLIO_PATH = path.join(DATA_DIR, 'portfolio.json');

// 确保文件夹存在
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

async function fetchETF(code) {
    try {
        const market = (code.startsWith('5') || code.startsWith('6')) ? '1' : '0';
        const quoteUrl = `https://push2.eastmoney.com/api/qt/stock/get?secid=${market}.${code}&fields=f43,f170,f47,f58`;
        const navUrl = `https://fundgz.1234567.com.cn/js/${code}.js?rt=${Date.now()}`;

        const [qRes, navText] = await Promise.all([
            fetch(quoteUrl).then(r => r.json()),
            fetch(navUrl).then(r => r.text()).catch(() => '')
        ]);

        if (!qRes.data) return null;

        const navMatch = navText.match(/jsonpgz\((.+)\)/);
        const navData = navMatch ? JSON.parse(navMatch[1]) : null;
        
        const price = qRes.data.f43 / 1000;
        const nav = navData ? parseFloat(navData.dwjz) : 0;
        
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
            vol: (qRes.data.f47 / 10000).toFixed(2)
        };
    } catch (e) { return null; }
}

async function run() {
    console.log('开始执行折价策略筛选...');
    const etfPath = path.join(process.cwd(), 'etf.txt');
    
    if (!fs.existsSync(etfPath)) {
        console.error('未找到 etf.txt 文件');
        return;
    }

    const codes = fs.readFileSync(etfPath, 'utf-8')
        .split(/\r?\n/)
        .map(l => l.trim())
        .filter(l => /^\d{6}$/.test(l));
    
    const results = [];
    for (let i = 0; i < codes.length; i += CONCURRENCY) {
        const batch = await Promise.all(codes.slice(i, i + CONCURRENCY).map(fetchETF));
        results.push(...batch.filter(Boolean));
        console.log(`进度: ${Math.min(i + CONCURRENCY, codes.length)}/${codes.length}`);
    }

    const timestamp = new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });

    // --- 1. 持仓追踪：仅记录负溢价（折价）信号 ---
    let portfolio = {};
    if (fs.existsSync(PORTFOLIO_PATH)) {
        portfolio = JSON.parse(fs.readFileSync(PORTFOLIO_PATH, 'utf-8'));
    }

    results.forEach(r => {
        // 修改点：只记录溢价率小于 -1.4 的机会
        if (r.premiumRaw < -1.4 && !portfolio[r.code]) {
            portfolio[r.code] = {
                name: r.name,
                buyPrice: r.price,
                buyDate: timestamp.split(' ')[0],
                status: '高折价买入(机会)'
            };
            console.log(`[信号触发] 发现折价标的: ${r.name} (${r.code})，溢价率: ${r.premium}`);
        }
    });
    fs.writeFileSync(PORTFOLIO_PATH, JSON.stringify(portfolio, null, 2));

    // 生成盈亏报表 (Profit Tracker)
    const profitHeader = '\ufeff代码,名称,买入日期,买入价格,当前价格,盈亏比例,更新时间\n';
    const profitRows = Object.keys(portfolio).map(code => {
        const current = results.find(r => r.code === code);
        const p = portfolio[code];
        if (!current) return null;
        const profitPct = ((current.price - p.buyPrice) / p.buyPrice * 100).toFixed(2) + '%';
        return `${code},${p.name},${p.buyDate},${p.buyPrice},${current.price.toFixed(3)},${profitPct},${timestamp}`;
    }).filter(Boolean).join('\n');
    
    fs.writeFileSync(path.join(DATA_DIR, 'profit_tracker.csv'), profitHeader + profitRows);

    // --- 2. 生成全量行情数据 ---
    const header = '\ufeff代码,名称,价格,估算净值,溢价率,涨跌幅,成交额(万),更新时间\n';
    const allRows = results.map(r => 
        `${r.code},${r.name},${r.price.toFixed(3)},${r.nav},${r.premium},${r.change},${r.vol},${timestamp}`
    ).join('\n');
    fs.writeFileSync(path.join(DATA_DIR, 'all_valid_data.csv'), header + allRows);

    // --- 3. 生成折价预警报告 ---
    const alertHeader = '\ufeff代码,名称,价格,溢价率,涨跌幅,更新时间\n';
    const alertRows = results
        .filter(r => r.premiumRaw < -1.5) // 修改点：仅筛选负溢价
        .sort((a, b) => a.premiumRaw - b.premiumRaw) // 按折价程度排序
        .map(r => `${r.code},${r.name},${r.price.toFixed(3)},${r.premium},${r.change},${timestamp}`)
        .join('\n');
    
    fs.writeFileSync(path.join(DATA_DIR, 'high_premium_alert.csv'), alertHeader + alertRows);

    console.log(`\n任务完成！`);
    console.log(`- 发现折价预警: ${alertRows ? alertRows.split('\n').length : 0} 条`);
    console.log(`- 盈亏追踪总数: ${Object.keys(portfolio).length} 条`);
}

run();
