import requests
import json
import zlib
import base64
import time
from datetime import datetime
import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import gzip
import shutil
from typing import Optional, List, Dict

class ForesightCrawler:
    def __init__(self):
        self.base_url = "https://api.foresightnews.pro/v1/event/53"
        self.page_size = 20
        self.data_dir = Path("foresight_data")
        self.max_retries = 3
        self.max_workers = 5
        self.setup_logging()
        
    def setup_logging(self):
        """配置日志系统"""
        self.data_dir.mkdir(exist_ok=True)
        log_file = self.data_dir / f"crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.DEBUG,  # 改为DEBUG级别
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def retry_on_failure(self, func, *args, **kwargs):
        """带重试机制的函数执行器"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                wait_time = (attempt + 1) * 2  # 递增等待时间
                self.logger.warning(f"尝试 {attempt + 1}/{self.max_retries} 失败: {e}, {wait_time}秒后重试")
                if attempt < self.max_retries - 1:
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"达到最大重试次数: {func.__name__}")
                    raise
                    
    def decrypt_data(self, encrypted_msg: str) -> Optional[Dict]:
        """解密API返回的数据"""
        try:
            # 打印原始加密数据的信息
            self.logger.debug(f"Original encrypted message length: {len(encrypted_msg)}")
            self.logger.debug(f"Encrypted message sample: {encrypted_msg[:100]}")
            self.logger.debug(f"Encrypted message encoding: {type(encrypted_msg)}")
            
            # Base64解码
            try:
                decoded = base64.b64decode(encrypted_msg)
                self.logger.debug(f"Base64 decoded length: {len(decoded)}")
                self.logger.debug(f"Decoded data sample (hex): {decoded.hex()[:100]}")
            except Exception as e:
                self.logger.error(f"Base64 decode failed: {e}")
                # 尝试修复base64填充
                try:
                    padding_fixed = encrypted_msg + '=' * (-len(encrypted_msg) % 4)
                    self.logger.debug(f"Trying with padding: {len(padding_fixed)}")
                    decoded = base64.b64decode(padding_fixed)
                except Exception as e2:
                    self.logger.error(f"Base64 decode with padding failed: {e2}")
                    return None
                
            # Zlib解压尝试
            decompression_methods = [
                (zlib.decompress, {"wbits": zlib.MAX_WBITS}),
                (zlib.decompress, {"wbits": -zlib.MAX_WBITS}),
                (zlib.decompress, {"wbits": 31}),  # For gzip
                (zlib.decompress, {"wbits": 15}),  # Raw deflate
            ]
            
            for decompress_func, kwargs in decompression_methods:
                try:
                    self.logger.debug(f"Trying decompression with params: {kwargs}")
                    decompressed = decompress_func(decoded, **kwargs)
                    self.logger.debug(f"Decompression successful with params: {kwargs}")
                    self.logger.debug(f"Decompressed length: {len(decompressed)}")
                    self.logger.debug(f"Decompressed sample: {decompressed[:100]}")
                    break
                except Exception as e:
                    self.logger.error(f"Decompression failed with params {kwargs}: {e}")
            else:
                self.logger.error("All decompression attempts failed")
                return None
            
            # JSON解析
            try:
                data = json.loads(decompressed)
                self.logger.debug(f"JSON parse successful, keys: {list(data.keys())}")
                return data
            except Exception as e:
                self.logger.error(f"JSON parse failed: {e}")
                self.logger.debug(f"Failed JSON data: {decompressed[:200]}")
                return None
            
        except Exception as e:
            self.logger.error(f"解密过程出现未知错误: {e}")
            import traceback
            self.logger.error(f"Error traceback: {traceback.format_exc()}")
            return None
            
    def validate_data(self, data: Dict):
        """验证数据完整性和格式"""
        required_fields = ["id", "title", "items", "total"]
        for field in required_fields:
            if field not in data:
                raise ValueError(f"数据缺少必要字段: {field}")
                
        if not isinstance(data["items"], list):
            raise ValueError("items 字段必须是列表")
            
        for item in data["items"]:
            if "id" not in item or "published_at" not in item:
                raise ValueError("item 缺少必要字段")
                
    def fetch_page(self, page: int) -> Optional[Dict]:
        """获取单页数据"""
        params = {
            "page": page,
            "size": self.page_size,
            "sort_by": "desc"
        }
        
        def _fetch():
            self.logger.info(f"正在请求第{page}页数据...")
            resp = requests.get(self.base_url, params=params, timeout=10)
            
            # 打印详细的请求信息
            self.logger.debug(f"Request URL: {resp.url}")
            self.logger.debug(f"Response Status: {resp.status_code}")
            
            try:
                data = resp.json()
                self.logger.debug(f"Response Data: {json.dumps(data, ensure_ascii=False)}")
                
                if data.get("code") == 1:
                    if "data" not in data:
                        self.logger.error("Response missing 'data' field")
                        return None
                        
                    encrypted_data = data["data"]
                    if not encrypted_data:
                        self.logger.error("Empty data field")
                        return None
                        
                    return self.decrypt_data(encrypted_data)
                else:
                    self.logger.error(f"API返回错误代码: {data.get('code')}")
                    return None
                
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON解析失败: {e}")
                self.logger.debug(f"Raw Response: {resp.text[:1000]}")
                return None
            
        return self.retry_on_failure(_fetch)
        
    def fetch_pages_parallel(self, start_page: int, end_page: int) -> List[Dict]:
        """并行获取多个页面的数据"""
        results = []
        lock = threading.Lock()
        
        def _fetch_and_store(page):
            data = self.fetch_page(page)
            if data and data["items"]:
                with lock:
                    results.extend(data["items"])
                    self.logger.info(f"成功获取第{page}页数据")
                    
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(_fetch_and_store, range(start_page, end_page + 1))
            
        return results
        
    def compress_old_data(self):
        """压缩历史数据文件"""
        current_month = datetime.now().strftime('%Y%m')
        for file in self.data_dir.glob("foresight_data_*.json"):
            if current_month not in file.name:
                with open(file, 'rb') as f_in:
                    with gzip.open(str(file) + '.gz', 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                file.unlink()  # 删除原文件
                self.logger.info(f"压缩历史数据: {file.name}")
                
    def save_data(self, data: Dict, filename: str):
        """保存数据到文件"""
        filepath = self.data_dir / filename
        
        # 备份已存在的文件
        if filepath.exists():
            backup_path = filepath.with_suffix('.backup')
            shutil.copy2(filepath, backup_path)
            
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"数据保存成功: {filename}")
            
            # 删除备份
            if backup_path.exists():
                backup_path.unlink()
        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
            # 恢复备份
            if backup_path.exists():
                shutil.move(backup_path, filepath)
                
    def fetch_all(self) -> Optional[Dict]:
        """获取所有数据"""
        self.logger.info("开始获取所有数据...")
        
        # 获取第一页来确定总数
        first_page = self.fetch_page(1)
        if not first_page:
            return None
            
        total = first_page["total"]
        total_pages = (total + self.page_size - 1) // self.page_size
        self.logger.info(f"总数据量: {total}, 总页数: {total_pages}")
        
        # 并行获取剩余页面
        all_items = first_page["items"]
        if total_pages > 1:
            remaining_items = self.fetch_pages_parallel(2, total_pages)
            all_items.extend(remaining_items)
            
        # 构建完整数据结构
        result = {
            "id": first_page["id"],
            "title": first_page["title"],
            "items": all_items,
            "total": len(all_items),
            "last_update": int(time.time())
        }
        
        # 验证数据完整性
        if len(all_items) != total:
            self.logger.warning(f"数据不完整: 预期{total}条, 实际获取{len(all_items)}条")
            
        # 保存数据
        filename = f"foresight_data_{datetime.now().strftime('%Y%m%d')}.json"
        self.save_data(result, filename)
        
        # 压缩历史数据
        self.compress_old_data()
        
        return result
        
    def check_updates(self) -> List[Dict]:
        """检查更新"""
        self.logger.info("开始检查更新...")
        
        # 获取最新的本地数据文件
        files = sorted(
            [f for f in self.data_dir.glob("foresight_data_*.json")],
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        
        if not files:
            self.logger.info("未找到本地数据,执行完整获取")
            self.fetch_all()
            return []
            
        # 加载最新数据
        try:
            with open(files[0], 'r', encoding='utf-8') as f:
                last_data = json.load(f)
        except Exception as e:
            self.logger.error(f"加载本地数据失败: {e}")
            return []
            
        # 获取新数据并比对
        new_data = self.fetch_page(1)
        if not new_data:
            return []
            
        # 通过published_at比对找出新增内容
        last_update = max(item["published_at"] for item in last_data["items"])
        new_items = [
            item for item in new_data["items"]
            if item["published_at"] > last_update
        ]
        
        if new_items:
            self.logger.info(f"发现{len(new_items)}条新内容")
            # 更新数据
            last_data["items"] = new_items + last_data["items"]
            last_data["total"] = len(last_data["items"])
            last_data["last_update"] = int(time.time())
            
            filename = f"foresight_data_{datetime.now().strftime('%Y%m%d')}.json"
            self.save_data(last_data, filename)
        else:
            self.logger.info("未发现新内容")
            
        return new_items

def main():
    crawler = ForesightCrawler()
    
    try:
        # 首次运行获取所有数据
        result = crawler.fetch_all()
        if result:
            crawler.logger.info(f"获取数据成功,共{result['total']}条")
        
        # 后续定时执行检查更新
        while True:
            try:
                new_items = crawler.check_updates()
                if new_items:
                    crawler.logger.info(f"更新成功,新增{len(new_items)}条内容")
                time.sleep(300)  # 5分钟检查一次
            except Exception as e:
                crawler.logger.error(f"检查更新失败: {e}")
                time.sleep(60)  # 发生错误时等待较短时间
                
    except KeyboardInterrupt:
        crawler.logger.info("程序终止")
    except Exception as e:
        crawler.logger.error(f"程序异常退出: {e}")
        raise
        
if __name__ == "__main__":
    main()