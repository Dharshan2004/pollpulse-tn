"""
Data management layer for Supabase Storage and Job Queue.

Implements the DataSystem class that handles:
- Uploading raw JSON data to Supabase Storage
- Creating job queue entries for downstream processing
"""

import json
from typing import Dict, Optional
from datetime import datetime

from .client import get_supabase_client


class DataSystem:
    """
    Manages data operations for the ETL pipeline.
    
    This class provides a unified interface for:
    1. Uploading raw JSON data to Supabase Storage (Data Lake)
    2. Creating job queue entries for asynchronous processing
    
    The decoupled architecture allows the Producer (scraper.py) to push
    data without waiting for the Consumer (processor.py) to process it.
    """
    
    def __init__(self, bucket_name: str = 'raw_data'):
        """
        Initialize the DataSystem.
        
        Args:
            bucket_name: Name of the Supabase Storage bucket to use
        """
        self.client = get_supabase_client()
        self.bucket_name = bucket_name
        
        if self.client is None:
            raise RuntimeError(
                "Supabase client not available. "
                "Ensure SUPABASE_URL and SUPABASE_KEY are set in .env"
            )
    
    def save_raw_json(
        self,
        data: Dict,
        filename: str,
        video_metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Save raw JSON data to Supabase Storage and create a job queue entry.
        
        This method implements the Producer pattern:
        1. Uploads JSON data to the Storage bucket (Data Lake)
        2. Creates a PENDING job in the job_queue table
        3. Returns the job ID for tracking
        
        Args:
            data: Dictionary containing the data to save (will be JSON serialized)
            filename: Filename to use in the storage bucket
            video_metadata: Optional metadata about the video (stored in job_queue.metadata)
        
        Returns:
            Job ID (UUID string) if successful, None otherwise
        """
        try:
            # Serialize data to JSON bytes
            json_content = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
            
            # Upload to Supabase Storage
            file_path = f"{filename}"
            
            storage_client = self.client.storage.from_(self.bucket_name)
            
            try:
                storage_client.upload(file_path, json_content)
            except Exception as storage_err:
                storage_msg = str(storage_err)
                error_lower = storage_msg.lower()
                
                if 'trailing slash' in error_lower:
                    pass
                elif 'row-level security' in error_lower or 'violates row-level security policy' in error_lower:
                    print(f"Storage RLS Error: Bucket '{self.bucket_name}' needs storage policies")
                    print("Run this SQL in Supabase SQL Editor:")
                    print(f"""
CREATE POLICY "Allow public uploads to {self.bucket_name}"
ON storage.objects FOR INSERT
TO anon, authenticated
WITH CHECK (bucket_id = '{self.bucket_name}');

CREATE POLICY "Allow public reads from {self.bucket_name}"
ON storage.objects FOR SELECT
TO anon, authenticated
USING (bucket_id = '{self.bucket_name}');
""")
                    return None
                else:
                    print(f"Storage error: {storage_msg}")
                    return None
            
            metadata = video_metadata or {}
            metadata.update({
                'filename': filename,
                'file_path': file_path,
                'uploaded_at': datetime.now().isoformat()
            })
            
            try:
                result = self.client.table('job_queue').insert({
                    'status': 'PENDING',
                    'file_path': file_path,
                    'metadata': metadata
                }).execute()
                
                if result.data and len(result.data) > 0:
                    job_id = result.data[0]['id']
                    print(f"Data saved to {file_path}, job {job_id} created")
                    return job_id
                else:
                    print("Warning: Job created but no ID returned")
                    return None
            except Exception as db_err:
                error_msg = str(db_err)
                error_lower = error_msg.lower()
                
                if 'row-level security' in error_lower or 'violates row-level security policy' in error_lower:
                    print(f"Database RLS Error: {error_msg}")
                    print("Run: ALTER TABLE job_queue DISABLE ROW LEVEL SECURITY;")
                else:
                    print(f"Database error: {error_msg}")
                return None
                
        except Exception as e:
            print(f"Error saving raw JSON: {str(e)}")
            return None
    
    def get_file_from_storage(self, file_path: str) -> Optional[Dict]:
        """
        Download and parse a JSON file from Supabase Storage.
        
        Args:
            file_path: Path to the file in the storage bucket
        
        Returns:
            Parsed JSON data as dictionary, None if error
        """
        try:
            response = self.client.storage.from_(self.bucket_name).download(file_path)
            data = json.loads(response.decode('utf-8'))
            return data
        except Exception as e:
            print(f"Error downloading file {file_path}: {str(e)}")
            return None
    
    def update_job_status(self, job_id: str, status: str):
        """
        Update the status of a job in the job_queue table.
        
        Args:
            job_id: UUID of the job to update
            status: New status (PENDING, PROCESSING, DONE, FAILED)
        """
        try:
            self.client.table('job_queue').update({
                'status': status
            }).eq('id', job_id).execute()
        except Exception as e:
            print(f"Error updating job status: {str(e)}")

