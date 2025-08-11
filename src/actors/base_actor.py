"""
Base actor class providing common functionality for all actors in the system.
"""

import json
import time
import logging
from typing import Any, Dict, Optional
from thespian.actors import Actor, ActorAddress
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ActorMessage:
    """Base message class for actor communication."""
    message_type: str
    data: Dict[str, Any]
    job_id: str
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class BaseActor(Actor):
    """Base actor class with common functionality."""
    
    def __init__(self):
        super().__init__()
        self.actor_name = self.__class__.__name__
        self.job_tracker = {}  # Track job states
        self.retry_count = {}  # Track retry attempts
        self.max_retries = 3
        
    def receiveMessage(self, message: ActorMessage, sender: ActorAddress) -> None:
        """Main message handler with error handling and retry logic."""
        try:
            logger.info(f"{self.actor_name} received message: {message.message_type}")
            
            # Track job
            if message.job_id not in self.job_tracker:
                self.job_tracker[message.job_id] = {
                    'start_time': time.time(),
                    'status': 'processing',
                    'retries': 0
                }
            
            # Process message
            result = self.process_message(message, sender)
            
            # Update job status
            self.job_tracker[message.job_id]['status'] = 'completed'
            self.job_tracker[message.job_id]['end_time'] = time.time()
            
            # Send response back to sender
            if sender:
                self.send(sender, result)
                
        except Exception as e:
            logger.error(f"Error in {self.actor_name}: {str(e)}")
            self.handle_error(message, sender, e)
    
    def process_message(self, message: ActorMessage, sender: ActorAddress) -> ActorMessage:
        """Override this method in subclasses to implement specific message processing."""
        raise NotImplementedError("Subclasses must implement process_message")
    
    def handle_error(self, message: ActorMessage, sender: ActorAddress, error: Exception) -> None:
        """Handle errors with retry logic."""
        job_id = message.job_id
        
        if job_id not in self.retry_count:
            self.retry_count[job_id] = 0
        
        if self.retry_count[job_id] < self.max_retries:
            self.retry_count[job_id] += 1
            logger.warning(f"Retrying {job_id} (attempt {self.retry_count[job_id]})")
            
            # Exponential backoff
            import time
            time.sleep(2 ** self.retry_count[job_id])
            
            # Retry the message
            self.receiveMessage(message, sender)
        else:
            # Max retries exceeded
            error_message = ActorMessage(
                message_type="error",
                data={
                    "error": str(error),
                    "job_id": job_id,
                    "actor": self.actor_name
                },
                job_id=job_id
            )
            
            if sender:
                self.send(sender, error_message)
            
            # Update job status
            self.job_tracker[job_id]['status'] = 'failed'
            self.job_tracker[job_id]['error'] = str(error)
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a specific job."""
        return self.job_tracker.get(job_id)
    
    def cleanup_job(self, job_id: str) -> None:
        """Clean up job tracking data."""
        if job_id in self.job_tracker:
            del self.job_tracker[job_id]
        if job_id in self.retry_count:
            del self.retry_count[job_id]
