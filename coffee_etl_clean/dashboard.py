"""
ETL PIPELINE DASHBOARD
======================
Giao diện web để theo dõi trực quan pipeline ETL

Features:
- Hiển thị trạng thái các zones (RAW, CLEAN, ERROR)
- Theo dõi số lượng files và records
- Real-time updates
- Xem nội dung files
- Statistics và charts

Usage:
    python dashboard.py
    
    Mở browser: http://localhost:5000
"""

from flask import Flask, render_template, jsonify, request
from pathlib import Path
import csv
import json
from datetime import datetime
from typing import Dict, List
import os
import subprocess

app = Flask(__name__)


class PipelineMonitor:
    """Monitor pipeline status và file changes."""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.staging_dir = self.base_dir / "staging"
        self.raw_dir = self.staging_dir / "raw"
        self.clean_dir = self.staging_dir / "clean"
        self.error_dir = self.staging_dir / "error"
        self.logs_dir = self.base_dir / "logs"
        
        # Tạo thư mục nếu chưa có
        for dir_path in [self.raw_dir, self.clean_dir, self.error_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def get_zone_stats(self, zone: str) -> Dict:
        """Lấy thống kê của một zone."""
        zone_map = {
            "raw": self.raw_dir,
            "clean": self.clean_dir,
            "error": self.error_dir
        }
        
        zone_dir = zone_map.get(zone)
        if not zone_dir or not zone_dir.exists():
            return {
                "zone": zone,
                "file_count": 0,
                "total_records": 0,
                "files": []
            }
        
        files = list(zone_dir.glob("*.csv"))
        total_records = 0
        file_details = []
        
        for file_path in sorted(files, key=lambda x: x.stat().st_mtime, reverse=True):
            try:
                # Đếm số records
                with open(file_path, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    row_count = sum(1 for _ in reader) - 1  # Trừ header
                
                total_records += row_count
                
                # Parse file name
                file_name = file_path.stem
                parts = file_name.split("_")
                
                # Xác định entity và source
                if len(parts) >= 2:
                    if parts[-2].isdigit():  # Có run_id
                        source = parts[-3]
                        entity = "_".join(parts[:-3])
                        run_id = "_".join(parts[-2:])
                    else:
                        source = parts[-1]
                        entity = "_".join(parts[:-1])
                        run_id = "unknown"
                else:
                    entity = file_name
                    source = "unknown"
                    run_id = "unknown"
                
                file_details.append({
                    "name": file_path.name,
                    "entity": entity,
                    "source": source,
                    "run_id": run_id,
                    "records": row_count,
                    "size": file_path.stat().st_size,
                    "modified": datetime.fromtimestamp(file_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                })
                
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
        
        return {
            "zone": zone,
            "file_count": len(files),
            "total_records": total_records,
            "files": file_details
        }
    
    def get_all_stats(self) -> Dict:
        """Lấy thống kê tất cả zones."""
        return {
            "raw": self.get_zone_stats("raw"),
            "clean": self.get_zone_stats("clean"),
            "error": self.get_zone_stats("error"),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def get_file_content(self, zone: str, filename: str, limit: int = 100) -> Dict:
        """Đọc nội dung file."""
        zone_map = {
            "raw": self.raw_dir,
            "clean": self.clean_dir,
            "error": self.error_dir
        }
        
        zone_dir = zone_map.get(zone)
        if not zone_dir:
            return {"error": "Invalid zone"}
        
        file_path = zone_dir / filename
        if not file_path.exists():
            return {"error": "File not found"}
        
        try:
            with open(file_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = []
                for i, row in enumerate(reader):
                    if i >= limit:
                        break
                    rows.append(row)
                
                return {
                    "filename": filename,
                    "zone": zone,
                    "columns": list(rows[0].keys()) if rows else [],
                    "rows": rows,
                    "total_shown": len(rows)
                }
        except Exception as e:
            return {"error": str(e)}
    
    def get_entity_summary(self) -> Dict:
        """Tổng hợp theo entity."""
        summary = {}
        
        for zone in ["raw", "clean", "error"]:
            stats = self.get_zone_stats(zone)
            
            for file_info in stats["files"]:
                entity = file_info["entity"]
                source = file_info["source"]
                key = f"{entity}_{source}"
                
                if key not in summary:
                    summary[key] = {
                        "entity": entity,
                        "source": source,
                        "raw": 0,
                        "clean": 0,
                        "error": 0
                    }
                
                summary[key][zone] = file_info["records"]
        
        # Tính toán success rate và validation status
        for key, data in summary.items():
            raw_count = data["raw"]
            clean_count = data["clean"]
            error_count = data["error"]
            
            if raw_count > 0:
                data["success_rate"] = round((clean_count / raw_count) * 100, 1)
                data["error_rate"] = round((error_count / raw_count) * 100, 1)
            else:
                data["success_rate"] = 0
                data["error_rate"] = 0
            
            # Status
            if error_count == 0:
                data["status"] = "perfect"  # 100% valid
            elif error_count < clean_count * 0.1:
                data["status"] = "good"  # <10% error
            elif error_count < clean_count * 0.3:
                data["status"] = "warning"  # 10-30% error
            else:
                data["status"] = "critical"  # >30% error
        
        return summary
    
    def get_logs(self, log_type: str = "pipeline", lines: int = 100) -> List[str]:
        """Đọc logs."""
        log_files = {
            "pipeline": self.logs_dir / "pipeline.log",
            "data": self.logs_dir / "data.log",
            "error": self.logs_dir / "error.log"
        }
        
        log_file = log_files.get(log_type)
        if not log_file or not log_file.exists():
            return []
        
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                all_lines = f.readlines()
                return all_lines[-lines:]  # Lấy N dòng cuối
        except Exception as e:
            return [f"Error reading log: {e}"]


# Global monitor instance
monitor = PipelineMonitor()


# ============================================================================
# ROUTES
# ============================================================================

@app.route("/")
def index():
    """Trang chủ dashboard."""
    return render_template("dashboard.html")


@app.route("/api/stats")
def api_stats():
    """API: Lấy thống kê tất cả zones."""
    return jsonify(monitor.get_all_stats())


@app.route("/api/entity-summary")
def api_entity_summary():
    """API: Tổng hợp theo entity."""
    return jsonify(monitor.get_entity_summary())


@app.route("/api/pipeline-info")
def api_pipeline_info():
    """API: Thông tin về pipeline và optimization."""
    return jsonify({
        "version": "2.0",
        "features": {
            "memory_optimization": True,
            "direct_load": True,
            "validation_rules": 106,
            "reject_on_error": True
        },
        "modes": {
            "pipeline": {
                "name": "Pipeline Mode (Memory)",
                "description": "Data truyền trực tiếp từ STEP 3 → STEP 4 qua memory",
                "speed": "Fast",
                "io_operations": 0
            },
            "standalone": {
                "name": "Standalone Mode (File)",
                "description": "Đọc từ staging/clean/*.csv",
                "speed": "Normal",
                "io_operations": 2
            }
        },
        "validation": {
            "reject_policy": "Dòng có BẤT KỲ lỗi nào (bao gồm cột rỗng) sẽ bị REJECT hoàn toàn",
            "no_transform": "Dòng INVALID KHÔNG được transform",
            "no_load": "Dòng INVALID KHÔNG được load vào SQL",
            "error_zone": "Dòng INVALID được ghi vào staging/error/ với error messages"
        }
    })


@app.route("/api/file-content")
def api_file_content():
    """API: Xem nội dung file."""
    zone = request.args.get("zone", "raw")
    filename = request.args.get("filename", "")
    limit = int(request.args.get("limit", 100))
    
    return jsonify(monitor.get_file_content(zone, filename, limit))


@app.route("/api/logs")
def api_logs():
    """API: Xem logs."""
    log_type = request.args.get("type", "pipeline")
    lines = int(request.args.get("lines", 100))
    
    return jsonify({
        "type": log_type,
        "lines": monitor.get_logs(log_type, lines)
    })


@app.route("/api/run-step", methods=["POST"])
def api_run_step():
    """API: Chạy một step của pipeline."""
    import sys
    
    data = request.get_json()
    step = data.get("step", "")
    
    step_files = {
        "step1": "STEP1_PRODUCER.py",
        "step2": "STEP2_RAW_CONSUMER.py",
        "step3": "STEP3_QUALITY_ENGINE.py",
        "step4": "STEP4_TRANSFORM_LOAD.py",
        "all": "RUN_ALL_STEPS.py",
        "direct": "PIPELINE_DIRECT_LOAD.py"  # Pipeline mới - Direct load
    }
    
    if step not in step_files:
        return jsonify({"success": False, "message": "Invalid step"}), 400
    
    script_file = step_files[step]
    script_path = monitor.base_dir / script_file
    
    try:
        # Chạy script trong console mới (Windows)
        if os.name == 'nt':
            # Windows: Sử dụng start command để mở console mới
            subprocess.Popen(
                f'start cmd /k "python {script_file}"',
                cwd=str(monitor.base_dir),
                shell=True
            )
        else:
            # Linux/Mac: Chạy trong background
            subprocess.Popen(
                [sys.executable, script_file],
                cwd=str(monitor.base_dir),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        
        return jsonify({
            "success": True,
            "message": f"Started {script_file}",
            "step": step
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}"
        }), 500


@app.route("/api/delete-file", methods=["POST"])
def api_delete_file():
    """API: Xóa một file."""
    data = request.get_json()
    zone = data.get("zone", "")
    filename = data.get("filename", "")
    
    zone_map = {
        "raw": monitor.raw_dir,
        "clean": monitor.clean_dir,
        "error": monitor.error_dir
    }
    
    zone_dir = zone_map.get(zone)
    if not zone_dir:
        return jsonify({"success": False, "message": "Invalid zone"}), 400
    
    file_path = zone_dir / filename
    
    if not file_path.exists():
        return jsonify({"success": False, "message": "File not found"}), 404
    
    try:
        file_path.unlink()
        return jsonify({
            "success": True,
            "message": f"Deleted {filename}"
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500


@app.route("/api/delete-zone", methods=["POST"])
def api_delete_zone():
    """API: Xóa tất cả files trong một zone."""
    data = request.get_json()
    zone = data.get("zone", "")
    
    zone_map = {
        "raw": monitor.raw_dir,
        "clean": monitor.clean_dir,
        "error": monitor.error_dir
    }
    
    zone_dir = zone_map.get(zone)
    if not zone_dir:
        return jsonify({"success": False, "message": "Invalid zone"}), 400
    
    try:
        files = list(zone_dir.glob("*.csv"))
        count = 0
        
        for file_path in files:
            file_path.unlink()
            count += 1
        
        return jsonify({
            "success": True,
            "message": f"Deleted {count} files from {zone.upper()} zone"
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500


@app.route("/api/download-file")
def api_download_file():
    """API: Download một file."""
    from flask import send_file
    
    zone = request.args.get("zone", "")
    filename = request.args.get("filename", "")
    
    zone_map = {
        "raw": monitor.raw_dir,
        "clean": monitor.clean_dir,
        "error": monitor.error_dir
    }
    
    zone_dir = zone_map.get(zone)
    if not zone_dir:
        return jsonify({"error": "Invalid zone"}), 400
    
    file_path = zone_dir / filename
    
    if not file_path.exists():
        return jsonify({"error": "File not found"}), 404
    
    return send_file(file_path, as_attachment=True)


if __name__ == "__main__":
    print()
    print("=" * 80)
    print("ETL PIPELINE DASHBOARD")
    print("=" * 80)
    print()
    print("🌐 Dashboard URL: http://localhost:5000")
    print()
    print("Features:")
    print("  • Real-time monitoring của RAW/CLEAN/ERROR zones")
    print("  • Xem nội dung files")
    print("  • Statistics và charts")
    print("  • Logs viewer")
    print()
    print("Press Ctrl+C to stop")
    print("=" * 80)
    print()
    
    app.run(debug=True, host="0.0.0.0", port=5000)
