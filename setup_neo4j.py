# setup_neo4j.py (نسخه نهایی با Fulltext Index)
from neo4j import GraphDatabase, exceptions

# --- تنظیمات اتصال ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "assist123"

class Neo4jSetup:
    def __init__(self, uri, user, password):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            self.driver.verify_connectivity()
            print("✅ اتصال به Neo4j با موفقیت برقرار شد.")
        except Exception as e:
            print(f"❌ خطای اتصال به Neo4j: {e}")
            exit()

    def close(self):
        self.driver.close()

    def setup_constraints_and_indexes(self):
        """
        ایجاد محدودیت‌های یکتایی و ایندکس جستجوی متنی.
        """
        commands = [
            # محدودیت‌های یکتایی (Constraints)
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chapter) REQUIRE c.chapter_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Group) REQUIRE g.group_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Item) REQUIRE i.item_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Rule) REQUIRE r.rule_id IS UNIQUE",
            
            # --- بخش جدید: ایندکس متن کامل (Fulltext Index) ---
            # این ایندکس به ما اجازه می‌دهد جستجوی دقیق کلمات (مانند "جدول") را انجام دهیم
            "CREATE FULLTEXT INDEX item_description_index IF NOT EXISTS FOR (n:Item) ON EACH [n.description]"
        ]

        try:
            with self.driver.session() as session:
                for i, command in enumerate(commands):
                    print(f"  ⏳ در حال اجرای دستور {i+1}...")
                    session.run(command)
                print("\n✅ تمام شِماها و ایندکس‌های متنی (Fulltext Index) با موفقیت اعمال شدند.")
        except Exception as e:
            print(f"❌ خطایی هنگام اعمال شِما رخ داد: {e}")

if __name__ == "__main__":
    print("--- شروع راه‌اندازی زیرساخت Neo4j ---")
    setup = Neo4jSetup(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    setup.setup_constraints_and_indexes()
    setup.close()
    print("--- عملیات پایان یافت ---")