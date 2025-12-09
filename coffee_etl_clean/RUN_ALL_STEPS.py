"""
RUN ALL STEPS
=============
Chạy toàn bộ pipeline từ đầu đến cuối

Luồng:
1. PRODUCER: CSV + SQL → RabbitMQ
2. RAW CONSUMER: RabbitMQ → staging/raw/*.csv
3. QUALITY ENGINE: staging/raw/ → staging/clean/ + staging/error/
4. TRANSFORM & LOAD: staging/clean/ → SQL Server

Usage:
    python RUN_ALL_STEPS.py
"""

import time
from datetime import datetime
from pathlib import Path

from etl.logger import logger

# Import các pipeline
from STEP1_PRODUCER import ProducerPipeline
from STEP2_RAW_CONSUMER import RawConsumerPipeline
from STEP3_QUALITY_ENGINE import QualityEnginePipeline
from STEP4_TRANSFORM_LOAD import TransformLoadPipeline


class FullPipeline:
    """Pipeline đầy đủ - Chạy tất cả các bước."""
    
    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.db_name = f"DB_{self.run_id}"
        self.start_time = None
        self.end_time = None
        self.step_times = {}  # Lưu thời gian từng bước
        self.step_results = {}  # Lưu kết quả từng bước
        self.pipeline_log_path = Path(f"staging/error/pipeline_run_{self.run_id}.log")
        
        # Tạo thư mục staging/error nếu chưa có
        self.pipeline_log_path.parent.mkdir(parents=True, exist_ok=True)
    
    def run(self):
        self.start_time = datetime.now()
        
        print()
        print("╔" + "=" * 78 + "╗")
        print("║" + " " * 25 + "FULL ETL PIPELINE" + " " * 36 + "║")
        print("║" + " " * 10 + "CSV/SQL → RabbitMQ → RAW → CLEAN/ERROR → SQL Server" + " " * 17 + "║")
        print("╚" + "=" * 78 + "╝")
        print()
        
        # Ghi log vào file pipeline
        self._write_pipeline_log("=" * 80)
        self._write_pipeline_log("FULL ETL PIPELINE - BẮT ĐẦU")
        self._write_pipeline_log("=" * 80)
        self._write_pipeline_log(f"Run ID: {self.run_id}")
        self._write_pipeline_log(f"Database: {self.db_name}")
        self._write_pipeline_log(f"Thời gian bắt đầu: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self._write_pipeline_log(f"Pipeline log file: {self.pipeline_log_path}")
        self._write_pipeline_log("=" * 80)
        
        logger.info("=" * 80)
        logger.info("FULL ETL PIPELINE - BẮT ĐẦU")
        logger.info("=" * 80)
        logger.info("Run ID: %s", self.run_id)
        logger.info("Database: %s", self.db_name)
        logger.info("Thời gian bắt đầu: %s", self.start_time.strftime("%Y-%m-%d %H:%M:%S"))
        logger.info("Pipeline log file: %s", self.pipeline_log_path)
        logger.info("=" * 80)
        
        try:
            # STEP 1: PRODUCER
            self._run_step_1()
            
            # Đợi messages được gửi xong
            logger.info("\n⏳ Đợi 2 giây để messages được gửi xong...")
            print("⏳ Đợi 2 giây để messages được gửi xong...")
            time.sleep(2)
            
            # STEP 2: RAW CONSUMER
            self._run_step_2()
            
            # STEP 3: QUALITY ENGINE
            validated_data = self._run_step_3()
            
            # STEP 4: TRANSFORM & LOAD
            self._run_step_4(validated_data)
            
            self.end_time = datetime.now()
            self.print_final_summary()
            
        except Exception as e:
            error_time = datetime.now()
            
            # Ghi lỗi vào pipeline log
            self._write_pipeline_log("\n" + "=" * 80)
            self._write_pipeline_log("❌ LỖI FULL PIPELINE")
            self._write_pipeline_log("=" * 80)
            self._write_pipeline_log(f"Lỗi: {str(e)}")
            self._write_pipeline_log(f"Thời điểm lỗi: {error_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self._write_pipeline_log(f"Các bước đã hoàn thành: {list(self.step_results.keys())}")
            self._write_pipeline_log("=" * 80)
            
            import traceback
            self._write_pipeline_log("\nChi tiết lỗi:")
            self._write_pipeline_log(traceback.format_exc())
            
            logger.error("\n" + "=" * 80)
            logger.error("❌ LỖI FULL PIPELINE")
            logger.error("=" * 80)
            logger.error("Lỗi: %s", str(e))
            logger.error("Thời điểm lỗi: %s", error_time.strftime("%Y-%m-%d %H:%M:%S"))
            logger.error("=" * 80, exc_info=True)
            print(f"\n❌ LỖI: {e}")
            print(f"📝 Chi tiết lỗi đã được ghi vào: {self.pipeline_log_path}")
            raise
    
    def _run_step_1(self):
        """Chạy STEP 1: PRODUCER"""
        step_start = datetime.now()
        
        self._write_pipeline_log("\n" + "╔" + "=" * 78 + "╗")
        self._write_pipeline_log("║  STEP 1: PRODUCER - CSV/SQL → RabbitMQ" + " " * 39 + "║")
        self._write_pipeline_log("╚" + "=" * 78 + "╝")
        self._write_pipeline_log(f"Bắt đầu: {step_start.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("\n" + "╔" + "=" * 78 + "╗")
        logger.info("║  STEP 1: PRODUCER - CSV/SQL → RabbitMQ" + " " * 39 + "║")
        logger.info("╚" + "=" * 78 + "╝")
        
        print("\n" + "╔" + "=" * 78 + "╗")
        print("║  STEP 1: PRODUCER - CSV/SQL → RabbitMQ" + " " * 39 + "║")
        print("╚" + "=" * 78 + "╝")
        
        try:
            step1 = ProducerPipeline()
            step1.run_id = self.run_id
            step1.run()
            
            step_end = datetime.now()
            duration = (step_end - step_start).total_seconds()
            self.step_times['step1'] = duration
            self.step_results['step1'] = 'SUCCESS'
            
            self._write_pipeline_log(f"✅ STEP 1 hoàn thành trong {duration:.2f} giây")
            self._write_pipeline_log(f"Kết thúc: {step_end.strftime('%Y-%m-%d %H:%M:%S')}")
            
            logger.info("✅ STEP 1 hoàn thành trong %.2f giây", duration)
            print(f"✅ STEP 1 hoàn thành trong {duration:.2f} giây\n")
            
        except Exception as e:
            self.step_results['step1'] = f'FAILED: {str(e)}'
            self._write_pipeline_log(f"❌ STEP 1 thất bại: {str(e)}")
            logger.error("❌ STEP 1 thất bại: %s", e, exc_info=True)
            raise
    
    def _run_step_2(self):
        """Chạy STEP 2: RAW CONSUMER"""
        step_start = datetime.now()
        
        self._write_pipeline_log("\n" + "╔" + "=" * 78 + "╗")
        self._write_pipeline_log("║  STEP 2: RAW CONSUMER - RabbitMQ → staging/raw/" + " " * 30 + "║")
        self._write_pipeline_log("╚" + "=" * 78 + "╝")
        self._write_pipeline_log(f"Bắt đầu: {step_start.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("\n" + "╔" + "=" * 78 + "╗")
        logger.info("║  STEP 2: RAW CONSUMER - RabbitMQ → staging/raw/" + " " * 30 + "║")
        logger.info("╚" + "=" * 78 + "╝")
        
        print("\n" + "╔" + "=" * 78 + "╗")
        print("║  STEP 2: RAW CONSUMER - RabbitMQ → staging/raw/" + " " * 30 + "║")
        print("╚" + "=" * 78 + "╝")
        
        try:
            step2 = RawConsumerPipeline()
            step2.run_id = self.run_id
            step2.run()
            
            step_end = datetime.now()
            duration = (step_end - step_start).total_seconds()
            self.step_times['step2'] = duration
            self.step_results['step2'] = 'SUCCESS'
            
            self._write_pipeline_log(f"✅ STEP 2 hoàn thành trong {duration:.2f} giây")
            self._write_pipeline_log(f"Kết thúc: {step_end.strftime('%Y-%m-%d %H:%M:%S')}")
            
            logger.info("✅ STEP 2 hoàn thành trong %.2f giây", duration)
            print(f"✅ STEP 2 hoàn thành trong {duration:.2f} giây\n")
            
        except Exception as e:
            self.step_results['step2'] = f'FAILED: {str(e)}'
            self._write_pipeline_log(f"❌ STEP 2 thất bại: {str(e)}")
            logger.error("❌ STEP 2 thất bại: %s", e, exc_info=True)
            raise
    
    def _run_step_3(self):
        """Chạy STEP 3: QUALITY ENGINE"""
        step_start = datetime.now()
        
        self._write_pipeline_log("\n" + "╔" + "=" * 78 + "╗")
        self._write_pipeline_log("║  STEP 3: QUALITY ENGINE - staging/raw/ → clean/error/" + " " * 23 + "║")
        self._write_pipeline_log("╚" + "=" * 78 + "╝")
        self._write_pipeline_log(f"Bắt đầu: {step_start.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("\n" + "╔" + "=" * 78 + "╗")
        logger.info("║  STEP 3: QUALITY ENGINE - staging/raw/ → clean/error/" + " " * 23 + "║")
        logger.info("╚" + "=" * 78 + "╝")
        
        print("\n" + "╔" + "=" * 78 + "╗")
        print("║  STEP 3: QUALITY ENGINE - staging/raw/ → clean/error/" + " " * 23 + "║")
        print("╚" + "=" * 78 + "╝")
        
        try:
            step3 = QualityEnginePipeline()
            step3.run_id = self.run_id
            validated_data = step3.run()
            
            step_end = datetime.now()
            duration = (step_end - step_start).total_seconds()
            self.step_times['step3'] = duration
            self.step_results['step3'] = 'SUCCESS'
            
            self._write_pipeline_log(f"✅ STEP 3 hoàn thành trong {duration:.2f} giây")
            self._write_pipeline_log(f"Kết thúc: {step_end.strftime('%Y-%m-%d %H:%M:%S')}")
            
            logger.info("✅ STEP 3 hoàn thành trong %.2f giây", duration)
            print(f"✅ STEP 3 hoàn thành trong {duration:.2f} giây\n")
            
            return validated_data
            
        except Exception as e:
            self.step_results['step3'] = f'FAILED: {str(e)}'
            self._write_pipeline_log(f"❌ STEP 3 thất bại: {str(e)}")
            logger.error("❌ STEP 3 thất bại: %s", e, exc_info=True)
            raise
    
    def _run_step_4(self, validated_data):
        """Chạy STEP 4: TRANSFORM & LOAD"""
        step_start = datetime.now()
        
        self._write_pipeline_log("\n" + "╔" + "=" * 78 + "╗")
        self._write_pipeline_log("║  STEP 4: TRANSFORM & LOAD - staging/clean/ → SQL Server" + " " * 20 + "║")
        self._write_pipeline_log("╚" + "=" * 78 + "╝")
        self._write_pipeline_log(f"Bắt đầu: {step_start.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("\n" + "╔" + "=" * 78 + "╗")
        logger.info("║  STEP 4: TRANSFORM & LOAD - staging/clean/ → SQL Server" + " " * 20 + "║")
        logger.info("╚" + "=" * 78 + "╝")
        
        print("\n" + "╔" + "=" * 78 + "╗")
        print("║  STEP 4: TRANSFORM & LOAD - staging/clean/ → SQL Server" + " " * 20 + "║")
        print("╚" + "=" * 78 + "╝")
        
        try:
            step4 = TransformLoadPipeline(db_name=self.db_name)
            step4.run_id = self.run_id
            step4.run(valid_data_from_memory=validated_data)
            
            step_end = datetime.now()
            duration = (step_end - step_start).total_seconds()
            self.step_times['step4'] = duration
            self.step_results['step4'] = 'SUCCESS'
            
            self._write_pipeline_log(f"✅ STEP 4 hoàn thành trong {duration:.2f} giây")
            self._write_pipeline_log(f"Kết thúc: {step_end.strftime('%Y-%m-%d %H:%M:%S')}")
            
            logger.info("✅ STEP 4 hoàn thành trong %.2f giây", duration)
            print(f"✅ STEP 4 hoàn thành trong {duration:.2f} giây\n")
            
        except Exception as e:
            self.step_results['step4'] = f'FAILED: {str(e)}'
            self._write_pipeline_log(f"❌ STEP 4 thất bại: {str(e)}")
            logger.error("❌ STEP 4 thất bại: %s", e, exc_info=True)
            raise
    
    def _write_pipeline_log(self, message):
        """Ghi log vào file pipeline trong staging/error/"""
        with open(self.pipeline_log_path, 'a', encoding='utf-8') as f:
            f.write(f"{message}\n")
    
    def print_final_summary(self):
        duration = (self.end_time - self.start_time).total_seconds()
        
        # Ghi summary vào pipeline log
        self._write_pipeline_log("\n" + "=" * 80)
        self._write_pipeline_log("🎉 FULL PIPELINE HOÀN THÀNH")
        self._write_pipeline_log("=" * 80)
        self._write_pipeline_log(f"\n📊 Thông tin:")
        self._write_pipeline_log(f"   • Run ID: {self.run_id}")
        self._write_pipeline_log(f"   • Database: {self.db_name}")
        self._write_pipeline_log(f"   • Thời gian bắt đầu: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self._write_pipeline_log(f"   • Thời gian kết thúc: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self._write_pipeline_log(f"   • Tổng thời gian: {duration:.2f} giây")
        
        self._write_pipeline_log(f"\n⏱️  Thời gian từng bước:")
        for step, time_taken in self.step_times.items():
            self._write_pipeline_log(f"   • {step.upper()}: {time_taken:.2f} giây")
        
        self._write_pipeline_log(f"\n✅ Kết quả từng bước:")
        for step, result in self.step_results.items():
            self._write_pipeline_log(f"   • {step.upper()}: {result}")
        
        self._write_pipeline_log(f"\n📁 Output Directories:")
        self._write_pipeline_log(f"   • RAW Zone: staging/raw/")
        self._write_pipeline_log(f"   • CLEAN Zone: staging/clean/")
        self._write_pipeline_log(f"   • ERROR Zone: staging/error/")
        
        self._write_pipeline_log(f"\n💾 SQL Server:")
        self._write_pipeline_log(f"   • Database: {self.db_name}")
        self._write_pipeline_log(f"   • Schema: staging")
        self._write_pipeline_log(f"   • Tables: *_csv, *_sql")
        
        self._write_pipeline_log(f"\n📝 Logs:")
        self._write_pipeline_log(f"   • Pipeline: logs/pipeline.log")
        self._write_pipeline_log(f"   • Data: logs/data.log")
        self._write_pipeline_log(f"   • Error: logs/error.log")
        self._write_pipeline_log(f"   • Pipeline Run Log: {self.pipeline_log_path}")
        
        self._write_pipeline_log(f"\n✅ Pipeline đã hoàn thành thành công!")
        self._write_pipeline_log("=" * 80)
        
        # Ghi vào logger
        logger.info("\n" + "=" * 80)
        logger.info("🎉 FULL PIPELINE HOÀN THÀNH")
        logger.info("=" * 80)
        
        logger.info("\n📊 Thông tin:")
        logger.info("   • Run ID: %s", self.run_id)
        logger.info("   • Database: %s", self.db_name)
        logger.info("   • Thời gian: %.2f giây", duration)
        
        logger.info("\n⏱️  Thời gian từng bước:")
        for step, time_taken in self.step_times.items():
            logger.info("   • %s: %.2f giây", step.upper(), time_taken)
        
        logger.info("\n📁 Output Directories:")
        logger.info("   • RAW Zone: staging/raw/")
        logger.info("   • CLEAN Zone: staging/clean/")
        logger.info("   • ERROR Zone: staging/error/")
        
        logger.info("\n💾 SQL Server:")
        logger.info("   • Database: %s", self.db_name)
        logger.info("   • Schema: staging")
        logger.info("   • Tables: *_csv, *_sql")
        
        logger.info("\n📝 Logs:")
        logger.info("   • Pipeline: logs/pipeline.log")
        logger.info("   • Data: logs/data.log")
        logger.info("   • Error: logs/error.log")
        logger.info("   • Pipeline Run Log: %s", self.pipeline_log_path)
        
        logger.info("\n✅ Pipeline đã hoàn thành thành công!")
        logger.info("=" * 80)
        
        # In ra console
        print()
        print("=" * 80)
        print("🎉 PIPELINE HOÀN THÀNH THÀNH CÔNG!")
        print("=" * 80)
        print(f"Run ID: {self.run_id}")
        print(f"Database: {self.db_name}")
        print(f"Thời gian: {duration:.2f} giây")
        print()
        print("⏱️  Thời gian từng bước:")
        for step, time_taken in self.step_times.items():
            print(f"   • {step.upper()}: {time_taken:.2f} giây")
        print()
        print("📁 Kiểm tra kết quả:")
        print(f"   • RAW Zone: staging/raw/")
        print(f"   • CLEAN Zone: staging/clean/")
        print(f"   • ERROR Zone: staging/error/")
        print(f"   • SQL Server: {self.db_name}.staging.*")
        print()
        print("📝 Pipeline log:")
        print(f"   • {self.pipeline_log_path}")
        print("=" * 80)
        print()


def main():
    pipeline = FullPipeline()
    
    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Pipeline bị dừng bởi user (Ctrl+C)")
    except Exception as e:
        logger.error("❌ Pipeline thất bại: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
