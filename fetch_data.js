const fs = require('fs');
const path = require('path');

const CONCURRENCY = 30; 

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
            price: price.toFixed(3),
            nav: nav.toFixed(4),
            premium: premiumVal.toFixed(2) + '%',
            premiumRaw: premiumVal,
            change: (qRes.data.f170 / 100).toFixed(2) + '%',
            vol: (qRes.data.f47 / 10000).toFixed(2)
        };
    } catch (e) { return null; }
}

async function run() {
    console.log('开始处理筛选任务...');
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
    const dataDir = path.join(process.cwd(), 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);

    const header = '\ufeff代码,名称,价格,估算净值,溢价率,涨跌幅,成交额(万),更新时间\n';
    const allRows = results.map(r => 
        `${r.code},${r.name},${r.price},${r.nav},${r.premium},${r.change},${r.vol},${timestamp}`
    ).join('\n');
    fs.writeFileSync(path.join(dataDir, 'all_valid_data.csv'), header + allRows);

    const alertHeader = '\ufeff代码,名称,价格,溢价率,涨跌幅,状态,更新时间\n';
    const alertRows = results
        .filter(r => Math.abs(r.premiumRaw) > 1.5)
        .sort((a, b) => Math.abs(b.premiumRaw) - Math.abs(a.premiumRaw))
        .map(r => {
            const status = r.premiumRaw > 0 ? '高溢价(风险)' : '高折价(机会)';
            return `${r.code},${r.name},${r.price},${r.premium},${r.change},${status},${timestamp}`;
        })
        .join('\n');
    
    fs.writeFileSync(path.join(dataDir, 'high_premium_alert.csv'), alertHeader + alertRows);

    console.log(`任务完成！有效净值数据: ${results.length} 条`);
}

run();
