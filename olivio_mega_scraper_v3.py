"""
╔══════════════════════════════════════════════════════════════════════════╗
║       OLIVIO&CO — MEGA SCRAPER v3.1 (Playwright ASYNC for Colab)       ║
║                                                                          ║
║  Google Colab'da çalıştırma:                                            ║
║    Cell 1:                                                               ║
║      !pip install playwright requests beautifulsoup4 lxml tqdm          ║
║      !pip install nest-asyncio                                           ║
║      !playwright install chromium                                        ║
║      !playwright install-deps                                            ║
║    Cell 2:                                                               ║
║      %run olivio_mega_scraper_v3.py                                     ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import subprocess, sys

def ensure_pkg(pkg, imp=None):
    try: __import__(imp or pkg)
    except ImportError: subprocess.check_call([sys.executable,'-m','pip','install',pkg,'-q'])

ensure_pkg('requests'); ensure_pkg('beautifulsoup4','bs4'); ensure_pkg('lxml')
ensure_pkg('tqdm'); ensure_pkg('nest-asyncio','nest_asyncio')

try:
    from playwright.async_api import async_playwright
except ImportError:
    subprocess.check_call([sys.executable,'-m','pip','install','playwright','-q'])
    subprocess.check_call([sys.executable,'-m','playwright','install','chromium'])
    try: subprocess.check_call([sys.executable,'-m','playwright','install-deps'])
    except: pass
    from playwright.async_api import async_playwright

import nest_asyncio; nest_asyncio.apply()
import asyncio, requests, json, re, time, os
from datetime import datetime
from collections import OrderedDict
from urllib.parse import urljoin
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

BASE_URL = "https://olivioandco.eu"
PRODUCTS_JSON_URL = f"{BASE_URL}/products.json"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
HEADERS = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36','Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8','Accept-Language':'en-US,en;q=0.9'}
DELAY = 0.35; PW_TIMEOUT = 15000; PW_TAB_WAIT = 1000
OUTPUT_FILE = "olivio_mega_v3.json"

session = requests.Session(); session.headers.update(HEADERS)

def safe_get(url, retries=3, **kw):
    kw.setdefault('timeout',20)
    for a in range(retries):
        try:
            r = session.get(url,**kw)
            if r.status_code==200: return r
            if r.status_code==429: time.sleep((a+1)*3); continue
            if r.status_code==404: return None
            time.sleep(1)
        except Exception as e:
            if a==retries-1: print(f"    ❌ {url} → {e}")
            time.sleep(2)
    return None

def clean_html(h):
    if not h: return ""
    return re.sub(r'\s+',' ',BeautifulSoup(h,'lxml').get_text(separator=' ',strip=True)).strip()

def norm_img(u):
    if not u: return None
    u = u.strip()
    if u.startswith('//'): u='https:'+u
    return u.split('?')[0] if '?' in u else u

# ============================================================
# PLAYWRIGHT ASYNC — TAB EXTRACTION
# ============================================================
async def extract_tabs_pw(page, url):
    """
    Olivio&Co tab yapısı:
    - Butonlar: <button class="main-product__tab" onclick="openTab(event,'tab-one-UUID','block-UUID')">
    - Paneller: <div id="tab-one-UUID"> içinde content
    - POPUP SORUNU: Newsletter + Klaviyo popup'ları tıklamayı engelliyor
    - ÇÖZÜM: Popup'ları JS ile kaldır, tab'ları JS openTab() ile aç
    """
    tabs = {"description":None,"materials":None,"size_guide":None,"shipping_and_customs":None}
    tab_map = {
        "description":["description"],
        "materials":["materials","material"],
        "size_guide":["size guide","size chart"],
        "shipping_and_customs":["shipping & customs","shipping and customs","shipping"],
    }
    
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=PW_TIMEOUT)
        await page.wait_for_timeout(2500)
        
        # ── POPUP'LARI KALDIR ──
        await page.evaluate("""
            () => {
                // Newsletter / Country popup
                document.querySelectorAll('.newsletter-popup, .country-popup, [id*="CountryPopup"], [id*="NewsletterPopup"]').forEach(el => el.remove());
                // Klaviyo popup
                document.querySelectorAll('[aria-label="POPUP Form"], .kl-private-reset-css-Xuajs1, [data-testid="form-component"]').forEach(el => {
                    let parent = el;
                    for (let i = 0; i < 5; i++) { if (parent.parentElement) parent = parent.parentElement; }
                    parent.remove();
                });
                // Genel modal overlay'ler
                document.querySelectorAll('[role="dialog"][aria-modal="true"], .modal-overlay, .popup-overlay').forEach(el => el.remove());
                // Shopify section popup'ları
                document.querySelectorAll('[id*="popup"] .shopify-section').forEach(el => el.remove());
            }
        """)
        await page.wait_for_timeout(300)
        
        # ── TAB BUTONLARINI BUL ──
        tab_data = await page.evaluate("""
            () => {
                const buttons = document.querySelectorAll('button.main-product__tab');
                const results = [];
                for (const btn of buttons) {
                    const text = btn.textContent.trim();
                    const onclick = btn.getAttribute('onclick') || '';
                    // openTab(event,'tab-one-UUID','block-UUID') pattern'inden ID'leri çıkar
                    const match = onclick.match(/openTab\\(event,\\s*'([^']+)',\\s*'([^']+)'\\)/);
                    const tabId = match ? match[1] : null;
                    const blockId = match ? match[2] : null;
                    
                    results.push({
                        text: text,
                        tabId: tabId,
                        blockId: blockId,
                        onclick: onclick,
                        index: results.length,
                    });
                }
                return results;
            }
        """)
        
        if not tab_data:
            return tabs
        
        # ── HER TAB İÇİN İÇERİĞİ AL ──
        for td in tab_data:
            btn_text = td.get('text','').strip()
            btn_lower = btn_text.lower()
            tab_id = td.get('tabId')
            block_id = td.get('blockId')
            
            matched_key = None
            for key, keywords in tab_map.items():
                if any(kw in btn_lower for kw in keywords):
                    matched_key = key; break
            if not matched_key:
                continue
            
            content_html = ""
            content_text = ""
            tab_images = []
            
            if tab_id:
                # JS ile tab'ı aç ve içeriğini oku
                result = await page.evaluate("""
                    (args) => {
                        const {tabId, blockId, btnIndex} = args;
                        
                        // openTab fonksiyonunu çağır (varsa)
                        try {
                            const btn = document.querySelectorAll('button.main-product__tab')[btnIndex];
                            if (btn) btn.click();
                        } catch(e) {}
                        
                        // Tab panelini ID ile bul
                        let panel = document.getElementById(tabId);
                        if (!panel && blockId) {
                            panel = document.getElementById(blockId);
                        }
                        
                        if (panel) {
                            return {
                                html: panel.innerHTML,
                                text: panel.innerText.trim(),
                            };
                        }
                        
                        // Fallback: tab-one veya block class'ına sahip elementi ara
                        const allDivs = document.querySelectorAll('[id^="tab-"], [id^="block-"]');
                        for (const d of allDivs) {
                            if (d.id === tabId || d.id === blockId) {
                                return {html: d.innerHTML, text: d.innerText.trim()};
                            }
                        }
                        
                        return null;
                    }
                """, {"tabId": tab_id, "blockId": block_id, "btnIndex": td.get('index',0)})
                
                if result:
                    content_html = result.get('html','')
                    content_text = result.get('text','')
            else:
                # onclick yoksa, force click ile dene
                try:
                    btn = page.locator('button.main-product__tab').nth(td.get('index',0))
                    await btn.click(force=True, timeout=3000)
                    await page.wait_for_timeout(PW_TAB_WAIT)
                    
                    # Aktif tab content'i bul
                    result = await page.evaluate("""
                        () => {
                            // Visible tab content
                            const panels = document.querySelectorAll('.main-product__tab-content, .tab-content, [class*="tab-content"]');
                            for (const p of panels) {
                                const s = window.getComputedStyle(p);
                                if (s.display !== 'none' && p.offsetHeight > 0) {
                                    return {html: p.innerHTML, text: p.innerText.trim()};
                                }
                            }
                            return null;
                        }
                    """)
                    if result:
                        content_html = result.get('html','')
                        content_text = result.get('text','')
                except:
                    pass
            
            # Footer filtre
            fws = ['about us','our blogs','contact us','privacy policy','newsletter']
            if content_text and sum(1 for fw in fws if fw in content_text.lower()) >= 2:
                continue
            
            # Tab içindeki görseller
            if content_html:
                sf = BeautifulSoup(content_html,'lxml')
                for ie in sf.find_all('img'):
                    src = norm_img(ie.get('src') or ie.get('data-src'))
                    if src: tab_images.append({"src":src,"alt":ie.get('alt','')})
            
            if content_text and len(content_text) > 3:
                tabs[matched_key] = {
                    "header": btn_text,
                    "content_text": content_text,
                    "content_html": content_html,
                    "images": tab_images,
                    "source": "playwright-openTab",
                }
    except Exception as e:
        pass
    
    return tabs


async def extract_swatches_pw(page):
    swatches = []
    try:
        result = await page.evaluate("""
            () => {
                const results = [];
                const sels = ['.color-swatches a','.color-swatches button','.swatch-element',
                    '.swatch__item','[data-option-name="Color"] .option-value',
                    '.product-colors a','.sibling__item a','.variant-option--color .option-value'];
                let elements = [];
                for (const sel of sels) {
                    const els = document.querySelectorAll(sel);
                    if (els.length>0) { elements=els; break; }
                }
                for (const el of elements) {
                    const cn = el.getAttribute('data-value')||el.getAttribute('title')
                        ||el.getAttribute('aria-label')||el.getAttribute('data-color')||el.innerText.trim();
                    if (!cn||cn.length>50) continue;
                    const info = {color_name:cn};
                    const img = el.querySelector('img');
                    if (img) info.swatch_image = img.src||img.dataset.src;
                    if (!info.swatch_image) {
                        const s = window.getComputedStyle(el);
                        const bg = s.backgroundImage;
                        if (bg&&bg!=='none') { const m=bg.match(/url\\(["']?(.*?)["']?\\)/); if(m) info.swatch_image=m[1]; }
                    }
                    if (!info.swatch_image) {
                        for (const ch of el.querySelectorAll('*')) {
                            const cs = window.getComputedStyle(ch);
                            const cbg = cs.backgroundImage;
                            if (cbg&&cbg!=='none') { const cm=cbg.match(/url\\(["']?(.*?)["']?\\)/); if(cm){info.swatch_image=cm[1];break;} }
                        }
                    }
                    const href = el.getAttribute('href')||(el.closest('a')||{}).href;
                    if (href&&href.includes('/products/')) info.product_url=href.split('?')[0];
                    const vid = el.getAttribute('data-variant-id')||el.getAttribute('value');
                    if (vid) info.variant_id=String(vid);
                    results.push(info);
                }
                return results;
            }
        """)
        if result:
            for s in result:
                if s.get('swatch_image'): s['swatch_image']=norm_img(s['swatch_image'])
                swatches.append(s)
    except: pass
    return swatches


async def extract_variant_images_pw(page):
    try:
        result = await page.evaluate("""
            () => {
                const sources = [window.product,
                    (window.ShopifyAnalytics||{}).meta&&window.ShopifyAnalytics.meta.product];
                for (const src of sources) {
                    if (src&&src.variants) {
                        const map={};
                        for (const v of src.variants) {
                            if (v.featured_image&&v.featured_image.src) map[String(v.id)]=v.featured_image.src;
                        }
                        if (Object.keys(map).length) return map;
                    }
                }
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d=JSON.parse(s.textContent); const p=d.product||d;
                        if (p&&p.variants) {
                            const map={};
                            for (const v of p.variants) {
                                if (v.featured_image) map[String(v.id)]=typeof v.featured_image==='string'?v.featured_image:v.featured_image.src;
                            }
                            if (Object.keys(map).length) return map;
                        }
                    } catch{}
                }
                return {};
            }
        """)
        if result: return {k:norm_img(v) for k,v in result.items() if v}
    except: pass
    return {}


# ============================================================
# STATIC SWATCH (CSS + CDN)
# ============================================================
def extract_swatches_static(soup, js_data, handle):
    swatches = {}
    if soup:
        for st in soup.find_all('style'):
            txt = st.string or ''
            for slug, img_url in re.findall(r'\.(?:swatch|color)[^{]*?--([^{\s]+)\s*\{[^}]*?background(?:-image)?\s*:\s*url\(["\']?(.*?)["\']?\)',txt,re.IGNORECASE):
                color = slug.replace('-',' ').replace('_',' ').strip().title()
                if color not in swatches:
                    swatches[color] = {'color_name':color,'swatch_image':norm_img(img_url),'swatch_source':'css-style-tag'}
    
    if js_data:
        for v in js_data.get('variants',[]):
            c = (v.get('option1') or '').strip()
            if not c: continue
            if c not in swatches: swatches[c]={'color_name':c}
            swatches[c].setdefault('variant_id',str(v.get('id','')))
            fi = v.get('featured_image')
            if fi:
                src = fi.get('src') if isinstance(fi,dict) else fi
                if src: swatches[c].setdefault('variant_image',norm_img(src))
    
    cdn = "https://cdn.shopify.com/s/files/1/0804/6411/8043/files/"
    for color, info in swatches.items():
        if info.get('swatch_image'): continue
        for slug in [color.lower().replace(' ','_'),color.lower().replace(' ','-'),color.lower().replace(' ','')]:
            for pat in [f'{slug}_36x.png',f'{slug}.png',f'swatch_{slug}.png']:
                try:
                    h = session.head(cdn+pat,timeout=5,allow_redirects=True)
                    if h.status_code==200 and 'image' in h.headers.get('content-type',''):
                        info['swatch_image']=cdn+pat; info['swatch_source']='cdn-pattern-match'; break
                except: continue
            if info.get('swatch_image'): break
    return swatches


# ============================================================
# ATTRIBUTES
# ============================================================
def parse_attributes(title, sku="", vtitles=None):
    t=title.lower().strip(); vtitles=vtitles or []
    age=None
    for p,l in [(r'\bteen\s*&\s*adult\b',"Teen & Adult"),(r'\bbaby\b',"Baby"),(r'\btoddler\b',"Toddler"),(r'\bkids?\b',"Kids"),(r'\bjunior\+?\b',"Junior"),(r'\badult\b',"Adult")]:
        if re.search(p,t): age=l; break
    shape=None
    for p,l in [(r'#d\b|d-frame',"D-Frame"),(r'\bfull-rim\b',"Full-Rim"),(r'\bhalf-rim\b',"Half-Rim"),(r'\bshield\b',"Shield"),(r'\boval\b',"Oval"),(r'\bround\b',"Round"),(r'\bsquare\b',"Square"),(r'\bcat-?eye\b',"Cat-Eye")]:
        if re.search(p,t): shape=l; break
    ptype=None
    for p,l in [(r'\bski\s+goggles?\b',"Ski Goggles"),(r'\bshield\s+sunglasses\b',"Shield Sunglasses"),(r'\bsports?\s+sunglasses\b',"Sports Sunglasses"),(r'\bscreen\s+glasses\b',"Screen Glasses"),(r'\bsunglasses\b',"Sunglasses"),(r'\bglasses\b',"Glasses"),(r'\bgoggles?\b',"Goggles"),(r'\baccessor|\bstrap\b|\bcase\b',"Accessories")]:
        if re.search(p,t): ptype=l; break
    if not ptype: ptype="Accessories"
    hp=hnp=False
    for vt in vtitles:
        vl=vt.lower()
        if 'non-polari' in vl or 'non polari' in vl: hnp=True
        elif 'polari' in vl: hp=True
    if 'EP' in (sku or ''): hp=True
    if re.search(r'polari[sz]',t): hp=True
    c=title.strip()
    for p in [r'\bTeen\s*&\s*Adult\b',r'\bBaby\b',r'\bToddler\b',r'\bKids?\b',r'\bJunior\+?\b',r'\bAdult\b',r'\bSki\s+Goggles?\b',r'\bShield\s+Sunglasses\b',r'\bSports?\s+Sunglasses\b',r'\bScreen\s+Glasses\b',r'\bSunglasses\b',r'\bGlasses\b',r'\bGoggles?\b',r'#D\b',r'\bD-Frame\b',r'\bFull-Rim\b',r'\bHalf-Rim\b',r'\bOval\b',r'\bRound\b',r'\bShield\b',r'\bSquare\b',r'\bCreative\b']:
        c=re.sub(p,'',c,flags=re.IGNORECASE)
    c=re.sub(r'[+\-–—]+',' ',c); c=re.sub(r'\s+',' ',c).strip().strip(' -–—+')
    return {"age_group":age,"frame_shape":shape,"product_type":ptype,"polarisation":{"has_polarised_option":hp,"has_non_polarised_option":hnp,"is_switchable":hp and hnp,"status":"switchable" if hp and hnp else "polarised_only" if hp else "non_polarised_only" if hnp else "unknown"},"extracted_color":c if c else None}


# ============================================================
# RECOMMENDATIONS, COLLECTIONS, PRODUCTS API, VARIANTS
# ============================================================
def fetch_recs(pid):
    if not pid: return [],"none"
    r=safe_get(f"{BASE_URL}/recommendations/products.json?product_id={pid}&limit=12")
    if not r: return [],"none"
    recs=[]
    try:
        for p in r.json().get('products',[]):
            pr=p.get('price')
            if isinstance(pr,(int,float)): pr=pr/100 if pr>500 else pr
            elif isinstance(pr,str):
                try: pf=float(pr.replace(',','.')); pr=pf/100 if pf>500 else pf
                except: pr=None
            else: pr=None
            recs.append({"id":p.get('id'),"title":p.get('title'),"handle":p.get('handle'),"url":f"{BASE_URL}/products/{p.get('handle')}","price":pr,"image":norm_img(p.get('featured_image') or (p.get('images',[None])[0] if p.get('images') else None))})
    except: pass
    return recs,"api" if recs else "none"

def fetch_collections():
    print("\n📂 Koleksiyonlar taranıyor...")
    cols={}
    sr=safe_get(SITEMAP_URL); cu=[]
    if sr:
        for su in re.findall(r'<loc>(.*?)</loc>',sr.text):
            if 'sitemap_collections' in su and '/fr/' not in su:
                sm=safe_get(su)
                if sm: cu.extend([l for l in re.findall(r'<loc>(.*?/collections/.*?)</loc>',sm.text) if '/fr/' not in l])
                time.sleep(DELAY*0.5)
    for n in ["all","frontpage","featured","best-selling","best-sellers","best-seller","news-2025","summer-sale","sunglasses","screenglasses","sport-sunglasses","ski-goggles","accessories","baby","toddler","kids","junior","adult"]:
        u=f"{BASE_URL}/collections/{n}"
        if u not in cu: cu.append(u)
    cu=list(set(cu))
    for col_url in tqdm(cu,desc="  Koleksiyonlar"):
        h=col_url.rstrip('/').split('/collections/')[-1].split('?')[0]
        ps=[]; pg=1
        while True:
            r=safe_get(f"{col_url}/products.json?limit=250&page={pg}")
            if not r: break
            try:
                p=r.json().get('products',[])
                if not p: break
                ps.extend(p)
                if len(p)<250: break
                pg+=1
            except: break
            time.sleep(DELAY*0.3)
        if ps: cols[h]={"url":col_url,"count":len(ps),"products":[{"position":i+1,"id":p.get('id'),"handle":p.get('handle'),"title":p.get('title')} for i,p in enumerate(ps)]}
        time.sleep(DELAY*0.3)
    print(f"  ✅ {len(cols)} koleksiyon")
    return cols

def get_col_pos(handle,pid,cols):
    pos={}
    for ch,col in cols.items():
        for p in col.get('products',[]):
            if p.get('handle')==handle or p.get('id')==pid:
                pos[ch]={"position":p['position'],"total":col['count']}; break
    fp=None
    for k in ['frontpage','featured']:
        if k in pos: fp=pos[k]['position']; break
    bp=None
    for k in ['best-seller','best-selling','best-sellers']:
        if k in pos: bp=pos[k]['position']; break
    return {"all_collections":pos,"is_featured":fp is not None,"featured_position":fp,"is_best_selling":bp is not None,"best_selling_position":bp}

def fetch_products():
    print("\n🔄 Products API...")
    ps=[]; pg=1
    while True:
        r=safe_get(f"{PRODUCTS_JSON_URL}?limit=250&page={pg}")
        if not r: break
        try:
            p=r.json().get('products',[])
            if not p: break
            ps.extend(p); print(f"  Sayfa {pg}: +{len(p)} (Toplam: {len(ps)})")
            if len(p)<250: break
            pg+=1
        except: break
        time.sleep(DELAY)
    print(f"  ✅ {len(ps)} ürün"); return ps

def enrich_variants(vraw,js_data,imgs,handle,pw_vi=None):
    js_vi={}
    if js_data:
        for jv in js_data.get('variants',[]):
            vid=jv.get('id'); fi=jv.get('featured_image')
            if fi and vid:
                src=fi.get('src') if isinstance(fi,dict) else fi
                if src: js_vi[vid]=norm_img(src)
    pw_vi=pw_vi or {}
    cm={}
    for img in (imgs or []):
        if not img: continue
        fn=img.split('/')[-1].lower()
        for v in vraw:
            c=(v.get('option1') or '').strip()
            if c and any(s in fn for s in [c.lower().replace(' ','-'),c.lower().replace(' ','_'),c.lower().replace(' ','')]):
                if c not in cm: cm[c]=img
    out=[]
    for v in vraw:
        vid=v.get('id'); c=(v.get('option1') or '').strip()
        fi=js_vi.get(vid) or pw_vi.get(str(vid)) or cm.get(c)
        fs=("js_data" if vid in js_vi else "playwright" if str(vid) in pw_vi else "color_filename_match" if c in cm else None)
        vt=(v.get('title') or '').lower()
        pol=None
        if 'non-polari' in vt or 'non polari' in vt: pol=False
        elif 'polari' in vt: pol=True
        elif 'EP' in (v.get('sku') or ''): pol=True
        out.append({"id":vid,"title":v.get('title'),"sku":v.get('sku'),"price":v.get('price'),"compare_at_price":v.get('compare_at_price'),"available":v.get('available',False),"option1":v.get('option1'),"option2":v.get('option2'),"option3":v.get('option3'),"is_polarised":pol,"featured_image":fi,"featured_image_source":fs,"variant_url":f"{BASE_URL}/products/{handle}?variant={vid}" if vid else None,"barcode":v.get('barcode')})
    return out


# ============================================================
# PROCESS PRODUCT (ASYNC)
# ============================================================
async def process_product(papi, cols, page):
    handle=papi.get('handle',''); pid=papi.get('id'); title=papi.get('title','')
    url=f"{BASE_URL}/products/{handle}"
    
    js_data=None
    jr=safe_get(f"{url}.js")
    if jr:
        try: js_data=jr.json()
        except: pass
    time.sleep(DELAY*0.15)
    
    soup=None
    hr=safe_get(url)
    if hr: soup=BeautifulSoup(hr.text,'lxml')
    time.sleep(DELAY*0.15)
    
    ai=[norm_img(i.get('src') if isinstance(i,dict) else i) for i in papi.get('images',[])]
    ji=[norm_img(i) for i in (js_data or {}).get('images',[])]
    all_imgs=list(OrderedDict.fromkeys([i for i in ai+ji if i]))
    
    fi=None
    aim=papi.get('image')
    if isinstance(aim,dict): fi=norm_img(aim.get('src'))
    elif isinstance(aim,str): fi=norm_img(aim)
    if not fi and all_imgs: fi=all_imgs[0]
    
    rb=papi.get('body_html',''); cd=clean_html(rb)
    vraw=papi.get('variants',[]); vtitles=[v.get('title','') for v in vraw]
    sku0=vraw[0].get('sku','') if vraw else ''
    attrs=parse_attributes(title,sku0,vtitles)
    recs,rsrc=fetch_recs(pid); time.sleep(DELAY*0.15)
    colpos=get_col_pos(handle,pid,cols)
    ssw=extract_swatches_static(soup,js_data,handle)
    
    tabs={"description":None,"materials":None,"size_guide":None,"shipping_and_customs":None}
    pw_sw=[]; pw_vi={}
    if page:
        try:
            tabs=await extract_tabs_pw(page,url)
            pw_sw=await extract_swatches_pw(page)
            pw_vi=await extract_variant_images_pw(page)
        except: pass
    
    if not tabs.get('description') and cd:
        di=[]
        if rb:
            s2=BeautifulSoup(rb,'lxml')
            for ie in s2.find_all('img'):
                src=norm_img(ie.get('src') or ie.get('data-src'))
                if src: di.append({"src":src,"alt":ie.get('alt','')})
        tabs['description']={"header":"Description","content_text":cd,"content_html":rb,"images":di,"source":"api-body-html"}
    
    merged={}
    for c,info in ssw.items(): merged[c]=info
    for s in pw_sw:
        cn=s.get('color_name','')
        if cn:
            ex=merged.get(cn,{'color_name':cn})
            for k in ['swatch_image','variant_image','product_url','variant_id']:
                if s.get(k) and not ex.get(k): ex[k]=s[k]
            merged[cn]=ex
    
    ev=enrich_variants(vraw,js_data,all_imgs,handle,pw_vi)
    
    otags=papi.get('tags',[]); 
    if isinstance(otags,str): otags=[t.strip() for t in otags.split(',') if t.strip()]
    st=[v for v in [attrs['age_group'],attrs['frame_shape'],attrs['product_type']] if v]
    if attrs['polarisation']['has_polarised_option']: st.append("Polarised")
    if attrs['polarisation']['is_switchable']: st.append("Polarise-Switchable")
    if colpos.get('is_featured'): st.append("Featured")
    if colpos.get('is_best_selling'): st.append("Best Selling")
    
    prices=[v.get('price') for v in vraw if v.get('price') is not None]
    
    return {
        "id":pid,"title":title,"handle":handle,"url":url,
        "vendor":papi.get('vendor','OLIVIO&CO'),
        "created_at":papi.get('created_at'),"updated_at":papi.get('updated_at'),"published_at":papi.get('published_at'),
        "description":{"raw_html":rb,"clean_text":cd},
        "tabs":tabs,
        "product_type_original":papi.get('product_type',''),
        "tags_original":otags,"tags_enriched":list(OrderedDict.fromkeys(otags+st)),
        "attributes":attrs,
        "price":{"amount":prices[0] if prices else None,"compare_at":vraw[0].get('compare_at_price') if vraw else None,"currency":"EUR","varies":len(set(prices))>1,"min":min(prices) if prices else None,"max":max(prices) if prices else None},
        "images":{"all":all_imgs,"featured":fi,"count":len(all_imgs)},
        "color_swatches":list(merged.values()),
        "variants":ev,"variant_count":len(ev),
        "options":papi.get('options',[]),
        "polarisation_summary":attrs['polarisation'],
        "collection_positions":colpos,
        "recommendations":{"you_may_also_like":recs,"count":len(recs),"source":rsrc},
        "_scraped_at":datetime.now().isoformat(),
    }


# ============================================================
# ASYNC MAIN
# ============================================================
async def async_main():
    start=time.time()
    print("="*70); print("  🫒 OLIVIO&CO MEGA SCRAPER v3.1 — Playwright ASYNC"); print("="*70)
    print(f"  Hedef: {BASE_URL}"); print(f"  Zaman: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"); print("="*70)
    
    cols=fetch_collections()
    all_prods=fetch_products()
    
    print("\n  🌐 Playwright async browser başlatılıyor...")
    pw=await async_playwright().start()
    browser=await pw.chromium.launch(headless=True,args=['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu','--single-process'])
    ctx=await browser.new_context(viewport={'width':1440,'height':900},user_agent=HEADERS['User-Agent'])
    async def block_route(route):
        await route.abort()
    await ctx.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot}", block_route)
    await ctx.route("**/analytics*", block_route)
    await ctx.route("**/google-analytics*", block_route)
    page=await ctx.new_page()
    print("  ✅ Browser hazır")
    
    print(f"\n🔍 {len(all_prods)} ürün işleniyor..."); print("-"*70)
    
    mega=[]; errors=[]
    for i,prod in enumerate(all_prods):
        try:
            full=await process_product(prod,cols,page)
            mega.append(full)
        except Exception as e:
            errors.append({"handle":prod.get('handle','?'),"error":str(e)})
        
        if (i+1)%10==0 or i==len(all_prods)-1:
            el=time.time()-start; rate=(i+1)/el*60; rem=(len(all_prods)-i-1)/(rate/60) if rate>0 else 0
            mat=sum(1 for p in mega if p.get('tabs',{}).get('materials'))
            sg=sum(1 for p in mega if p.get('tabs',{}).get('size_guide'))
            sh=sum(1 for p in mega if p.get('tabs',{}).get('shipping_and_customs'))
            print(f"  [{i+1}/{len(all_prods)} {(i+1)/len(all_prods)*100:.0f}%] {rate:.0f}/dk ~{rem:.0f}s | Tabs M:{mat} SG:{sg} SC:{sh} | Err:{len(errors)}")
        await asyncio.sleep(DELAY*0.3)
    
    await page.close(); await ctx.close(); await browser.close(); await pw.stop()
    print("  🔒 Browser kapatıldı")
    
    output={"_metadata":{"source":BASE_URL,"scraped_at":datetime.now().isoformat(),"scraper_version":"3.1-playwright-async","total_products":len(mega),"total_variants":sum(p.get('variant_count',0) for p in mega),"total_images":sum(p.get('images',{}).get('count',0) for p in mega),"collections_scraped":list(cols.keys()),"errors":errors,"duration_seconds":round(time.time()-start,1)},"collections":cols,"products":mega}
    
    with open(OUTPUT_FILE,'w',encoding='utf-8') as f: json.dump(output,f,ensure_ascii=False,indent=2)
    flat=OUTPUT_FILE.replace('.json','_flat.json')
    with open(flat,'w',encoding='utf-8') as f: json.dump(mega,f,ensure_ascii=False,indent=2)
    
    el=time.time()-start
    print("\n"+"="*70); print("  📋 ÖZET RAPOR"); print("="*70)
    print(f"  Ürünler: {len(mega)} | Varyantlar: {sum(p.get('variant_count',0) for p in mega)} | Süre: {el:.0f}s ({el/60:.1f}dk)")
    
    print(f"\n  📑 Tab Doluluğu:")
    for tk in ['description','materials','size_guide','shipping_and_customs']:
        cnt=sum(1 for p in mega if p.get('tabs',{}).get(tk))
        srcs={}
        for p in mega:
            tab=p.get('tabs',{}).get(tk)
            if tab: srcs[tab.get('source','?')]=srcs.get(tab.get('source','?'),0)+1
        print(f"    {tk}: {cnt}/{len(mega)} — {srcs}")
    
    swt=sum(len(p.get('color_swatches',[])) for p in mega)
    swi=sum(1 for p in mega for s in p.get('color_swatches',[]) if s.get('swatch_image'))
    print(f"\n  🎨 Swatch: {swi}/{swt}")
    vit=sum(len(p.get('variants',[])) for p in mega)
    vif=sum(1 for p in mega for v in p.get('variants',[]) if v.get('featured_image'))
    print(f"  🖼️  Variant Image: {vif}/{vit}")
    bs=sum(1 for p in mega if p.get('collection_positions',{}).get('is_best_selling'))
    print(f"  🏆 Best Selling: {bs}")
    bad=sum(1 for p in mega if (p.get('attributes',{}).get('extracted_color') or '').startswith('+'))
    print(f"  🎨 Bozuk renk: {bad}")
    
    mb=os.path.getsize(OUTPUT_FILE)/1024/1024
    print(f"\n  💾 {OUTPUT_FILE} ({mb:.1f} MB)"); print("  ✅ TAMAMLANDI!"); print("="*70)
    return mega

def run():
    loop=asyncio.get_event_loop()
    return loop.run_until_complete(async_main())

def download():
    try:
        from google.colab import files
        files.download(OUTPUT_FILE); files.download(OUTPUT_FILE.replace('.json','_flat.json'))
    except ImportError: print(f"📁 Dosyalar: {os.getcwd()}")

if __name__=="__main__":
    data=run(); download()
