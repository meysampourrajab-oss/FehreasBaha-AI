# batch_analyze_chapter_01.py (نسخه ۱.۲ - اصلاح نهایی با Cypher UNION)
import json
from neo4j import GraphDatabase, exceptions
import vertexai
from vertexai.preview.generative_models import GenerativeModel, Part
import time

# --- ۱. تنظیمات اتصال ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "assist123" # <-- رمز عبور Neo4j
PROJECT_ID = "fehrest-baha-ai"        # <-- شناسه پروژه GCP
LOCATION = "us-central1"               # ( us-central1 یا us-east4)

# --- ۲. تعریف توابع (اصلاح شده) ---

def get_rule_context_from_neo4j(driver, rule_id: str) -> dict:
    """
    متن خام قانون و «بافتار» آن را از Neo4j می‌خواند.
    *** این تابع با یک پرس‌وجوی UNION بازنویسی شده است ***
    """
    with driver.session(database="BrainDB") as session:
        # پرس‌وجوی جدید با UNION برای مدیریت هر دو نوع قانون
        
        # بخش ۱: منطق برای قوانین گروهی
        query_part1 = """
            MATCH (rule:Rule {rule_id: $id})<-[:HAS_RULE]-(group:Group)
            MATCH (group)-[:BELONGS_TO]->(chapter:Chapter)
            MATCH (other_group:Group)-[:BELONGS_TO]->(chapter)
            OPTIONAL MATCH (item:Item)-[:BELONGS_TO]->(group)
            RETURN
                rule.raw_text AS raw_text,
                rule.scope AS scope,
                group.group_id AS parent_id,
                group.title AS parent_title,
                collect(DISTINCT item.item_id) AS items_in_group,
                collect(DISTINCT {id: other_group.group_id, title: other_group.title}) AS all_groups_in_chapter
        """
        
        # بخش ۲: منطق برای قوانین عمومی فصل
        query_part2 = """
            MATCH (rule:Rule {rule_id: $id})<-[:HAS_RULE]-(chapter:Chapter)
            OPTIONAL MATCH (other_group:Group)-[:BELONGS_TO]->(chapter)
            RETURN
                rule.raw_text AS raw_text,
                rule.scope AS scope,
                chapter.chapter_id AS parent_id,
                chapter.title AS parent_title,
                [] AS items_in_group, // قوانین عمومی، ردیف مستقیم در گروه خود ندارند
                collect(DISTINCT {id: other_group.group_id, title: other_group.title}) AS all_groups_in_chapter
        """
        
        # اجرای هر دو بخش پرس‌وجو
        result = session.run(f"{query_part1} UNION {query_part2}", id=rule_id)
        
        record = result.single()
        if record:
            print(f"✅ بافتار کامل قانون {rule_id} با موفقیت از Neo4j خوانده شد.")
            return dict(record)
        else:
            raise Exception(f"قانون {rule_id} یا بافتار آن در Neo4j یافت نشد.")

def analyze_rule_with_gemini(model, rule_text: str, context: dict) -> str:
    """
    متن خام قانون را به همراه «بافتار» کامل به Gemini API ارسال می‌کند.
    """
    print("🧠 در حال ارسال متن و بافتار به Vertex AI (Gemini) برای تحلیل دقیق...")
    
    # ساخت بخش بافتار برای پرامپت
    context_prompt = ""
    if context['scope'] == 'Group':
        context_prompt = f"""
        * قانون زیر متعلق به: `Group {context['parent_id']} (title: {context['parent_title']})`
        * ردیف‌های داخل این گروه عبارتند از: `{context['items_in_group']}`
        * لیست تمام گروه‌های این فصل (برای ارجاع): `{context['all_groups_in_chapter']}`
        """
    elif context['scope'] == 'General':
        context_prompt = f"""
        * قانون زیر یک «الزام عمومی» برای فصل است (ID: {context['parent_id']})`
        * لیست تمام گروه‌های این فصل (برای ارجاع): `{context['all_groups_in_chapter']}`
        """

    # --- پرامپت مهندسی‌شده ---
    prompt = f"""
    شما یک دستیار متخصص تحلیل فهرست بهای ابنیه هستید. وظیفه شما خواندن یک قانون و تبدیل آن به یک JSON کاملاً ساختاریافته و ماشین‌خوان است.

    **۱. بافتار (Context) از پایگاه داده گرافی (Neo4j):**
    {context_prompt}

    **۲. متن خام قانون (Rule Text) برای تحلیل:**
    "{rule_text}"

    **۳. شِما و دستورالعمل‌های خروجی (JSON Schema & Instructions):**
    شما باید خروجی را *دقیقاً* در قالب JSON زیر ارائه دهید:
    {{
      "rule_type": "نوع قانون (مثلاً: Reclassify, Computational, Negation, Informational, Reference)",
      "condition_logic": "یک رشته شرطی پایتون (Python conditional string) که قابل ارزیابی (evaluable) باشد. این شرط *فقط* باید از متغیرهای دقیق زیر استفاده کند: ['area_m2', 'groove_area_cm2', 'tool', 'method', 'dimension_m']. برای مقایسه 'tool' یا 'method'، *فقط* از کدهای استاندارد انگلیسی زیر استفاده کن: ['HEAVY_MACHINERY', 'COMPRESSOR', 'METHOD_DRILL', ...]. هرگز از کلمات فارسی، اپراتور 'in' یا لیست (براکت []) استفاده نکن. به جای 'in' از 'or' استفاده کن (مثال: 'method == "A" or method == "B"'). اگر شرطی وجود ندارد، 'False' را برگردان.",
      "action": {{
        "type": "نوع عملیات (مثلاً: RECLASSIFY_TO_GROUP, APPLY_COEFFICIENT, NEGATE_ITEM, REFER_TO_CHAPTER)",
        "value": "مقدار عملیات. اگر ارجاع است، *فقط شناسه (ID)* گروه یا فصل را از لیست بافتار بالا برگردان (مثلاً: '0104' یا '03')",
        "message": "متن هشداری که باید به کاربر نمایش داده شود"
      }},
      "affected_items": ["لیست *دقیق* ردیف‌هایی که در متن قانون ذکر شده‌اند (مثلاً: ['010204', '010205', '010206'])"],
      "cross_references": ["لیست *دقیق* شناسه‌های (ID) فصول یا گروه‌هایی که به آنها ارجاع داده شده (مثلاً: ['Group: 0104', 'Chapter: 03'])"]
    }}
    
    **دستورالعمل ویژه برای نویز (Filter Override):**
    اگر قانون فعلی صرفاً یک الزام عمومی، اطلاع‌رسانی، یا توضیحی باشد (مانند R-01-Gen-2)، شما باید **اجباری** مقادیر زیر را در خروجی JSON قرار دهید تا از شلوغی خروجی عملیاتی جلوگیری شود:
    - "rule_type": "Informational"
    - "condition_logic": "False"
    
    **۴. خروجی (فقط JSON معتبر):**
    """
    # --- پایان پرامپت ---

    try:
        # استفاده از مدلی که شما با موفقیت فعال کردید
        model_name = "gemini-2.5-pro"
        model = GenerativeModel(model_name)
        
        response = model.generate_content([prompt])
        json_output = response.text.strip().replace("```json", "").replace("```", "")
        json.loads(json_output) # تست اعتبارسنجی JSON
        print("✅ تحلیل هوشمند (با بافتار) با موفقیت دریافت شد.")
        return json_output
    
    except Exception as e:
        print(f"    ❌ خطایی در تحلیل Gemini رخ داد: {e}")
        return None

def update_rule_in_neo4j(driver, rule_id: str, logic_json: str):
    """
    گره قانون در Neo4j را با JSON منطقی جدید به‌روزرسانی می‌کند.
    """
    with driver.session(database="BrainDB") as session:
        session.run("""
            MATCH (r:Rule {rule_id: $id})
            SET r.logic_json = $json_text
        """, id=rule_id, json_text=logic_json)

def fetch_unanalyzed_rules(driver) -> list:
    """
    لیست تمام قوانینی که هنوز logic_json ندارند را برمی‌گرداند.
    """
    with driver.session(database="BrainDB") as session:
        # ما فقط قوانین فصل ۰۱ را می‌گیریم
        result = session.run("""
            MATCH (r:Rule)
            WHERE r.rule_id STARTS WITH 'R-01' AND r.logic_json IS NULL
            RETURN r.rule_id AS rule_id
            ORDER BY r.rule_id
            """)
        return [record["rule_id"] for record in result]

# --- ۳. اجرای اسکریپت ---
if __name__ == "__main__":
    
    print("--- شروع فرآیند تحلیل دسته‌ای قوانین فصل ۰۱ ---")
    
    # اتصال به GCP
    try:
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        print("✅ اتصال به Vertex AI (Gemini) با موفقیت برقرار شد.")
    except Exception as e:
        print(f"❌ خطای اتصال به Vertex AI: {e}")
        exit()

    # اتصال به Neo4j
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        print("✅ اتصال به Neo4j با موفقیت برقرار شد.")
    except Exception as e:
        print(f"❌ خطای اتصال به Neo4j: {e}")
        exit()

    # ۱. گرفتن لیست قوانین تحلیل نشده
    try:
        rules_to_analyze = fetch_unanalyzed_rules(driver)
        
        print(f"\n💡 {len(rules_to_analyze)} قانون تحلیل نشده در فصل ۰۱ یافت شد.")
        if not rules_to_analyze:
            print("🟢 به نظر می‌رسد تمام قوانین فصل ۰۱ قبلاً تحلیل شده‌اند.")
            
        # ۲. حلقه تحلیل (Loop)
        for i, rule_id in enumerate(rules_to_analyze):
            print(f"\n--- در حال پردازش قانون {i+1} از {len(rules_to_analyze)}: [{rule_id}] ---")
            try:
                # ۳. گرفتن بافتار
                context = get_rule_context_from_neo4j(driver, rule_id)
                
                # ۴. تحلیل با AI
                print(f"🧠 در حال ارسال '{context['raw_text'][:50]}...' به Gemini...")
                logical_json = analyze_rule_with_gemini(None, context['raw_text'], context) # مدل را در داخل تابع می‌سازیم
                
                if logical_json:
                    # ۵. ذخیره در Neo4j
                    update_rule_in_neo4j(driver, rule_id, logical_json)
                    print(f"✅ قانون {rule_id} با موفقیت در Neo4j به‌روزرسانی شد.")
                
                # تاخیر جزئی برای جلوگیری از خطای Rate Limit در API
                time.sleep(1) 
                
            except Exception as e:
                print(f"❌ خطایی در پردازش قانون {rule_id} رخ داد: {e}")
                
    except Exception as e:
        print(f"❌ عملیات با خطا مواجه شد: {e}")
    
    finally:
        driver.close()
        print("\n--- عملیات تحلیل دسته‌ای پایان یافت ---")