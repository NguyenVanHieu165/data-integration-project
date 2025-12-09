"""
STEP 3: QUALITY ENGINE
======================
Đọc staging/raw/*.csv → Validate → Phân loại vào CLEAN/ERROR zones

Luồng:
- staging/raw/*.csv → Quality Engine (106 rules) → staging/clean/*.csv + staging/error/*.csv

Output:
- staging/clean/*.csv (Valid records)
- staging/error/*.csv (Invalid records với error messages)
"""

import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set

from etl.quality.rule_registry import rule_registry
from etl.logger import logger


class QualityEnginePipeline:
    """Pipeline Quality Engine - Validate và phân loại dữ liệu."""
    
    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Thư mục
        self.raw_dir = Path("staging") / "raw"
        self.clean_dir = Path("staging") / "clean"
        self.error_dir = Path("staging") / "error"
        
        self.clean_dir.mkdir(parents=True, exist_ok=True)
        self.error_dir.mkdir(parents=True, exist_ok=True)
        
        self.stats = {}
        
        # Store validated data in memory để truyền cho STEP 4
        self.validated_data = {}  # {entity_source: [valid_rows]}
    
    def run(self):
        logger.info("=" * 80)
        logger.info("STEP 3: QUALITY ENGINE PIPELINE")
        logger.info("Run ID: %s", self.run_id)
        logger.info("Input: staging/raw/")
        logger.info("Output: staging/clean/ + staging/error/ + Memory")
        logger.info("=" * 80)
        
        try:
            # Tìm tất cả CSV files trong raw/
            raw_files = list(self.raw_dir.glob("*.csv"))
            
            if not raw_files:
                logger.warning("⚠️  Không tìm thấy file nào trong staging/raw/")
                return {}
            
            logger.info("\n📄 Found %s raw files", len(raw_files))
            
            for raw_file in sorted(raw_files):
                logger.info("\n📥 Processing: %s", raw_file.name)
                self.process_file(raw_file)
            
            self.print_summary()
            
            # Trả về validated data để STEP 4 sử dụng
            logger.info("\n✅ Validated data ready in memory for STEP 4")
            return self.validated_data
            
        except Exception as e:
            logger.error("❌ Lỗi Quality Engine pipeline: %s", e, exc_info=True)
            raise
    
    def process_file(self, raw_file: Path):
        """Xử lý một raw file."""
        # Parse file name: entity_source_runid.csv
        file_name = raw_file.stem  # Bỏ .csv
        parts = file_name.split("_")
        
        # Xác định entity type và source
        if len(parts) >= 2:
            # Trường hợp: khach_hang_csv_20251209_123456
            if parts[-2].isdigit():  # Có run_id
                source = parts[-3]  # csv hoặc sql
                entity_type = "_".join(parts[:-3])  # khach_hang
            else:
                source = parts[-1]  # csv hoặc sql
                entity_type = "_".join(parts[:-1])  # khach_hang
        else:
            logger.warning("   ⚠️  Không parse được file name: %s", file_name)
            return
        
        logger.info("   Entity: %s | Source: %s", entity_type, source)
        
        # Đọc raw file
        try:
            with open(raw_file, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception as e:
            logger.error("   ✗ Lỗi đọc file: %s", e)
            return
        
        logger.info("   Total rows: %s", len(rows))
        
        # Validate từng row
        valid_rows = []
        error_rows = []
        
        # Context cho validation (track IDs, emails để check duplicate)
        seen_ids = set()
        seen_emails = set()
        
        for i, row in enumerate(rows, 1):
            # Loại bỏ metadata columns (_source, _extract_time, _run_id)
            data = {k: v for k, v in row.items() if not k.startswith("_")}
            
            # Validate
            is_valid, fixed_row, errors = rule_registry.validate_row(
                entity_type=entity_type,
                row=data,
                context={
                    "existing_ids": seen_ids,
                    "existing_emails": seen_emails,
                    "source": source
                }
            )
            
            if is_valid:
                # ✅ VALID: Thêm vào clean zone
                valid_rows.append(fixed_row)
                
                # Track IDs và emails để check duplicate
                for id_field in ["id", "customer_id", "ma_nguyen_lieu", "ma_loai"]:
                    if id_field in fixed_row and fixed_row[id_field]:
                        try:
                            seen_ids.add(int(fixed_row[id_field]))
                        except (ValueError, TypeError):
                            pass
                
                if "email" in fixed_row and fixed_row["email"]:
                    seen_emails.add(fixed_row["email"].lower())
            else:
                # ❌ INVALID: Thêm vào error zone
                # Dòng này sẽ KHÔNG được transform và KHÔNG được load vào SQL
                error_row = {**data}
                error_row["_errors"] = " | ".join(errors)
                error_row["_row_number"] = i
                error_rows.append(error_row)
        
        # Ghi vào clean/error files
        clean_file = self.clean_dir / f"{entity_type}_{source}_{self.run_id}.csv"
        error_file = self.error_dir / f"{entity_type}_{source}_{self.run_id}.csv"
        
        if valid_rows:
            # ✅ Chỉ các rows VALID được ghi vào CLEAN zone
            # Các rows này sẽ được transform và load vào SQL ở STEP 4
            self.write_csv(clean_file, valid_rows)
            logger.info("   ✓ Clean: %s rows → %s", len(valid_rows), clean_file.name)
        
        if error_rows:
            # ❌ Các rows INVALID được ghi vào ERROR zone
            # Các rows này sẽ KHÔNG được transform và KHÔNG được load vào SQL
            self.write_csv(error_file, error_rows)
            logger.info("   ✗ Error: %s rows → %s", len(error_rows), error_file.name)
        
        # Stats
        self.stats[file_name] = {
            "total": len(rows),
            "valid": len(valid_rows),
            "invalid": len(error_rows),
            "entity": entity_type,
            "source": source
        }
        
        # Store validated data in memory
        key = f"{entity_type}_{source}"
        self.validated_data[key] = valid_rows
    
    def write_csv(self, file_path: Path, rows: List[Dict]):
        """Ghi rows vào CSV file."""
        if not rows:
            return
        
        with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    
    def print_summary(self):
        logger.info("\n" + "=" * 80)
        logger.info("📊 QUALITY ENGINE SUMMARY")
        logger.info("=" * 80)
        
        logger.info("\n📁 Output Directories:")
        logger.info("   • Clean: %s", self.clean_dir)
        logger.info("   • Error: %s", self.error_dir)
        
        logger.info("\n📊 Validation Results:")
        total_all = 0
        total_valid = 0
        total_invalid = 0
        
        for file_name, stats in sorted(self.stats.items()):
            logger.info("   • %s (%s):", stats["entity"], stats["source"])
            logger.info("     Total: %s | Valid: %s | Invalid: %s", 
                       stats["total"], 
                       stats["valid"], 
                       stats["invalid"])
            
            total_all += stats["total"]
            total_valid += stats["valid"]
            total_invalid += stats["invalid"]
        
        if total_all > 0:
            logger.info("\n✅ TỔNG KẾT:")
            logger.info("   • Total: %s rows", total_all)
            logger.info("   • Valid: %s rows (%.1f%%)", 
                       total_valid, 
                       total_valid / total_all * 100)
            logger.info("   • Invalid: %s rows (%.1f%%)", 
                       total_invalid, 
                       total_invalid / total_all * 100)
        
        logger.info("=" * 80)


def main():
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 25 + "STEP 3: QUALITY ENGINE" + " " * 32 + "║")
    print("║" + " " * 18 + "staging/raw/ → staging/clean/ + error/" + " " * 23 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    pipeline = QualityEnginePipeline()
    
    try:
        validated_data = pipeline.run()
        return validated_data
    except KeyboardInterrupt:
        logger.info("\n⚠️  Pipeline bị dừng bởi user (Ctrl+C)")
        return {}
    except Exception as e:
        logger.error("❌ Pipeline thất bại: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
