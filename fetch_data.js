const fs = require('fs');
const path = require('path');

const CONCURRENCY = 30; // 维持并发效率

async function fetchETF(code) {
    try {
        // 判断市场：5/6 开头为沪市 (1)，其他为深市 (0)
        const market = (code.startsWith('5') || code.startsWith('6')) ? '1' : '0';
        
        // 抓取核心行情（价格、涨跌幅、名称、成交额）
        const quoteUrl = `https://push2.eastmoney.com/api/qt/stock/get?secid=${market}.${code}&fields=f43,f170,f47,f58`;
        // 抓取实时估算净值
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
        
        // 如果没有净值数据，无法计算溢价，直接过滤掉
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
    } catch (e) { 
        return null; 
    }
}

async function run() {
    console.log('--- 正在从 etf.txt 读取基金代码 ---');
    const etfPath = path.join(process.cwd(), 'etf.txt');
    
    // 检查文件是否存在
    if (!fs.existsSync(etfPath)) {
        console.error('错误：当前目录下未找到 etf.txt 文件！');
        return;
    }

    // 读取并清洗代码数据
    const codes = fs.readFileSync(etfPath, 'utf-8')
        .split(/\r?\n/)
        .map(l => l.trim())
        .filter(l => /^\d{6}$/.test(l)); [cite: 1]

    if (codes.length === 0) {
        console.log('未在 etf.txt 中找到有效的 6 位基金代码。');
        return;
    }

    console.log(`已加载 ${codes.length} 个代码，开始处理...`);
    
    const results = [];
    for (let i = 0; i < codes.length; i += CONCURRENCY) {
        const batch = await Promise.all(codes.slice(i, i + CONCURRENCY).map(fetchETF));
        results.push(...batch.filter(Boolean));
        console.log(`进度: ${Math.min(i + CONCURRENCY, codes.length)}/${codes.length}`);
    }

    // 生成输出
    const timestamp = new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });
    const dataDir = path.join(process.cwd(), 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);

    // 1. 全量数据 CSV
    const header = '\ufeff代码,名称,价格,估算净值,溢价率,涨跌幅,成交额(万),更新时间\n';
    const allRows = results.map(r => 
        `${r.code},${r.name},${r.price},${r.nav},${r.premium},${r.change},${r.vol},${timestamp}`
    ).join('\n');
    fs.writeFileSync(path.join(dataDir, 'all_valid_data.csv'), header + allRows);

    // 2. 溢价预警 (> 1.5%)
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

    console.log(`\n任务完成！`);
    console.log(`- 成功获取有效净值: ${results.length} 条`);
    console.log(`- 触发 1.5% 溢价预警: ${alertRows ? alertRows.split('\n').length : 0} 条`);
    console.log(`- 结果已保存至 /data 文件夹`);
}

run();
