# etl/discovery/csv_discovery.py
"""
CSV Discovery - Tự động phát hiện và phân loại CSV files
"""
from pathlib import Path
from typing import List, Dict
from ..logger import logger


class CSVDiscovery:
    """Auto-discover CSV files trong thư mục."""
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
    
    def discover_all(self) -> List[Dict]:
        """
        Phát hiện tất cả CSV files.
        
        Returns:
            List of dict: [
                {
                    "file_path": "data/khachhang.csv",
                    "file_name": "khachhang.csv",
                    "entity_type": "khach_hang",
                    "queue_name": "queue_khach_hang"
                }
            ]
        """
        if not self.data_dir.exists():
            logger.warning("Thư mục không tồn tại: %s", self.data_dir)
            return []
        
        csv_files = list(self.data_dir.glob("*.csv"))
        
        results = []
        for csv_file in csv_files:
            entity_type = self._infer_entity_type(csv_file.stem)
            
            results.append({
                "file_path": str(csv_file),
                "file_name": csv_file.name,
                "entity_type": entity_type,
                "queue_name": f"queue_{entity_type}",
                "staging_table": f"staging.{entity_type}_tbl"
            })
        
        logger.info("Phát hiện %s CSV files", len(results))
        return results
    
    def _infer_entity_type(self, file_stem: str) -> str:
        """
        Suy luận entity type từ tên file.
        
        Examples:
            khachhang.csv -> khach_hang
            nguyen_lieu_tbl.csv -> nguyen_lieu
            mon_tbl.csv -> mon
        """
        # Mapping tên file -> entity type
        mapping = {
            "khachhang": "khach_hang",
            "khach_hang": "khach_hang",
            "khach_hang_tbl": "khach_hang",
            
            "nguyenlieu": "nguyen_lieu",
            "nguyen_lieu": "nguyen_lieu",
            "nguyen_lieu_tbl": "nguyen_lieu",
            
            "loaimon": "loai_mon",
            "loai_mon": "loai_mon",
            "loai_mon_tbl": "loai_mon",
            "loaisanpham": "loai_mon",  # Thêm mapping cho loaisanpham.csv
            
            "tensanpham": "mon",  # tensanpham.csv -> mon (món ăn)
            "ten_san_pham": "mon",
            "ten_san_pham_tbl": "mon",
            
            "dathang": "dat_hang",
            "dat_hang": "dat_hang",
            "dat_hang_tbl": "dat_hang",
            
            "mon": "mon",
            "mon_tbl": "mon",
            
            "sysdiagrams": "sysdiagrams"  # Skip system tables
        }
        
        normalized = file_stem.lower().replace("-", "_").replace(" ", "_")
        return mapping.get(normalized, normalized)
    
    @staticmethod
    def get_latest_extract_dir(output_dir: str) -> str:
        """
        Lấy thư mục extract mới nhất từ output/.
        
        Args:
            output_dir: Đường dẫn thư mục output
        
        Returns:
            Đường dẫn thư mục extract mới nhất
        """
        output_path = Path(output_dir)
        
        if not output_path.exists():
            raise FileNotFoundError(f"Thư mục không tồn tại: {output_dir}")
        
        # Tìm tất cả thư mục extract_*
        extract_dirs = sorted(output_path.glob("extract_*"), reverse=True)
        
        if not extract_dirs:
            raise FileNotFoundError(f"Không tìm thấy thư mục extract_* trong: {output_dir}")
        
        latest = extract_dirs[0]
        logger.info("Thư mục extract mới nhất: %s", latest.name)
        
        return str(latest)
