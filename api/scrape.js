import { chromium } from 'playwright-core';
import chromiumBinary from '@sparticuz/chromium';
import { kv } from '@vercel/kv';

export default async function handler(req, res) {
  if (req.method !== 'GET' && req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const email = process.env.WATERSCOPE_EMAIL;
  const password = process.env.WATERSCOPE_PASSWORD;

  if (!email || !password) {
    return res.status(500).json({ error: 'Missing WATERSCOPE_EMAIL or WATERSCOPE_PASSWORD env vars' });
  }

  let browser;
  try {
    browser = await chromium.launch({
      args: chromiumBinary.args,
      defaultViewport: chromiumBinary.defaultViewport,
      executablePath: await chromiumBinary.executablePath(),
      headless: chromiumBinary.headless,
    });

    const page = await browser.newPage();

    // Navigate to waterscope.us — Azure B2C will redirect to login
    await page.goto('https://waterscope.us', { waitUntil: 'networkidle' });

    // Fill Azure B2C login form
    await page.fill('input[type="email"], input[name="email"], #email', email);
    await page.fill('input[type="password"], input[name="password"], #password', password);
    await page.click('button[type="submit"], input[type="submit"], #next');

    // Wait for dashboard to load after login
    await page.waitForSelector('#meterpanetopheaderbar', { timeout: 30000 });

    // Scrape the meter stats from the header bar
    const data = await page.evaluate(() => {
      const bar = document.querySelector('#meterpanetopheaderbar');
      if (!bar) return null;

      const getText = (selector) => {
        const el = bar.querySelector(selector);
        return el ? el.textContent.trim() : null;
      };

      // Try to extract labeled values — adjust selectors if waterscope updates their DOM
      const allText = bar.innerText;

      // Parse key/value pairs from the bar text
      const result = {};

      const lcdMatch = allText.match(/LCD Read[:\s]+([^\n]+)/i);
      const dailyMatch = allText.match(/Daily Average[:\s]+([^\n]+)/i);
      const cycleMatch = allText.match(/So Far This Cycle[:\s]+([^\n]+)/i);
      const budgetMatch = allText.match(/Water Budget[:\s]+([^\n]+)/i);

      if (lcdMatch) result.lcdRead = lcdMatch[1].trim();
      if (dailyMatch) result.dailyAverage = dailyMatch[1].trim();
      if (cycleMatch) result.soFarThisCycle = cycleMatch[1].trim();
      if (budgetMatch) result.waterBudgetStatus = budgetMatch[1].trim();

      result.rawText = allText;
      return result;
    });

    if (!data) {
      return res.status(500).json({ error: 'Could not find #meterpanetopheaderbar on page' });
    }

    const payload = {
      ...data,
      scrapedAt: new Date().toISOString(),
    };

    await kv.set('waterwise:latest', payload);

    return res.status(200).json({ ok: true, data: payload });
  } catch (err) {
    console.error('Scrape error:', err);
    return res.status(500).json({ error: err.message });
  } finally {
    if (browser) await browser.close();
  }
}
