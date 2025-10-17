import os
import logging
import asyncio
from typing import Dict, List, Optional
from aiohttp import web
import threading
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    ContextTypes
)
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from PIL import Image, ImageDraw, ImageFont
import io
import time
from enum import Enum
import re
import pytesseract

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
WAITING_MESSAGE, WAITING_BUTTONS, WAITING_PROTECTION, WAITING_TARGET = range(4)
WAITING_TEMPLATE_NAME, WAITING_TEMPLATE_MESSAGE, WAITING_TEMPLATE_CATEGORY = range(4, 7)
WAITING_SCHEDULE_TIME, WAITING_SCHEDULE_REPEAT = range(7, 9)
WAITING_ADMIN_ID, WAITING_ADMIN_ROLE = range(9, 11)
WAITING_SIGNAL_MESSAGE = 11

# New states for verification
(
    WAITING_VIP_GROUP,
    WAITING_ACCOUNT_CREATION_CONFIRMATION,
    WAITING_ACCOUNT_DATE,
    WAITING_CR_NUMBER,
    WAITING_SCREENSHOT,
    WAITING_KENNEDYNESPOT_CONFIRMATION,
    WAITING_BROKER_CHOICE,
    WAITING_ACCOUNT_NAME,
    WAITING_ACCOUNT_NUMBER,
    WAITING_TELEGRAM_ID,
) = range(12, 22)


class AdminRole(Enum):
    """Admin role definitions"""
    SUPER_ADMIN = "super_admin"
    ADMIN = "admin"
    MODERATOR = "moderator"
    BROADCASTER = "broadcaster"

class Permission(Enum):
    """Permission definitions"""
    BROADCAST = "broadcast"
    MANAGE_ADMINS = "manage_admins"
    VIEW_STATS = "view_stats"
    MANAGE_TEMPLATES = "manage_templates"
    APPROVE_BROADCASTS = "approve_broadcasts"
    VIEW_LOGS = "view_logs"
    MANAGE_USERS = "manage_users"
    SCHEDULE_BROADCASTS = "schedule_broadcasts"

# Role permissions mapping
ROLE_PERMISSIONS = {
    AdminRole.SUPER_ADMIN: [
        Permission.BROADCAST,
        Permission.MANAGE_ADMINS,
        Permission.VIEW_STATS,
        Permission.MANAGE_TEMPLATES,
        Permission.APPROVE_BROADCASTS,
        Permission.VIEW_LOGS,
        Permission.MANAGE_USERS,
        Permission.SCHEDULE_BROADCASTS
    ],
    AdminRole.ADMIN: [
        Permission.BROADCAST,
        Permission.VIEW_STATS,
        Permission.MANAGE_TEMPLATES,
        Permission.VIEW_LOGS,
        Permission.MANAGE_USERS,
        Permission.SCHEDULE_BROADCASTS
    ],
    AdminRole.MODERATOR: [
        Permission.VIEW_STATS,
        Permission.MANAGE_TEMPLATES,
        Permission.VIEW_LOGS,
        Permission.APPROVE_BROADCASTS
    ],
    AdminRole.BROADCASTER: [
        Permission.BROADCAST,
        Permission.MANAGE_TEMPLATES,
        Permission.SCHEDULE_BROADCASTS
    ]
}


class MongoDBHandler:
    """Handle all MongoDB operations"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.client = None
        self.db = None
        self.users_collection = None
        self.subscribers_collection = None
        self.admins_collection = None
        self.templates_collection = None
        self.scheduled_broadcasts_collection = None
        self.activity_logs_collection = None
        self.broadcast_approvals_collection = None
        self.signal_suggestions_collection = None
        self.connect()

    def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000
            )
            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client['telegram_bot']
            self.users_collection = self.db['users']
            self.subscribers_collection = self.db['subscribers']
            self.admins_collection = self.db['admins']
            self.templates_collection = self.db['templates']
            self.scheduled_broadcasts_collection = self.db['scheduled_broadcasts']
            self.activity_logs_collection = self.db['activity_logs']
            self.broadcast_approvals_collection = self.db['broadcast_approvals']
            self.signal_suggestions_collection = self.db['signal_suggestions']

            # Create indexes
            self.users_collection.create_index('user_id', unique=True)
            self.subscribers_collection.create_index('user_id', unique=True)
            self.admins_collection.create_index('user_id', unique=True)
            self.templates_collection.create_index('created_by')
            self.scheduled_broadcasts_collection.create_index('scheduled_time')
            self.activity_logs_collection.create_index([('timestamp', -1)])
            self.broadcast_approvals_collection.create_index('status')
            self.signal_suggestions_collection.create_index('status')

            logger.info("Successfully connected to MongoDB")
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def add_user(self, user_id: int, username: str = None, first_name: str = None):
        """Add or update a user"""
        try:
            self.users_collection.update_one(
                {'user_id': user_id},
                {
                    '$set': {
                        'user_id': user_id,
                        'username': username,
                        'first_name': first_name,
                        'last_interaction': time.time()
                    },
                    '$setOnInsert': {'created_at': time.time()}
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding user {user_id}: {e}")
            return False

    def add_subscriber(self, user_id: int):
        """Add a subscriber"""
        try:
            self.subscribers_collection.update_one(
                {'user_id': user_id},
                {
                    '$set': {
                        'user_id': user_id,
                        'subscribed_at': time.time()
                    }
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error subscribing user {user_id}: {e}")
            return False

    def remove_subscriber(self, user_id: int):
        """Remove a subscriber"""
        try:
            result = self.subscribers_collection.delete_one({'user_id': user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error unsubscribing user {user_id}: {e}")
            return False

    def is_subscriber(self, user_id: int) -> bool:
        """Check if user is a subscriber"""
        try:
            return self.subscribers_collection.find_one({'user_id': user_id}) is not None
        except Exception as e:
            logger.error(f"Error checking subscriber status for {user_id}: {e}")
            return False

    def get_all_users(self) -> set:
        """Get all user IDs"""
        try:
            users = self.users_collection.find({}, {'user_id': 1})
            return {user['user_id'] for user in users}
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return set()

    def get_all_subscribers(self) -> set:
        """Get all subscriber IDs"""
        try:
            subscribers = self.subscribers_collection.find({}, {'user_id': 1})
            return {sub['user_id'] for sub in subscribers}
        except Exception as e:
            logger.error(f"Error getting all subscribers: {e}")
            return set()

    def get_all_admin_ids(self) -> set:
        """Get all admin user IDs"""
        try:
            admins = self.admins_collection.find({}, {'user_id': 1})
            return {admin['user_id'] for admin in admins}
        except Exception as e:
            logger.error(f"Error getting all admin IDs: {e}")
            return set()

    def get_stats(self) -> Dict:
        """Get bot statistics"""
        try:
            total_users = self.users_collection.count_documents({})
            total_subscribers = self.subscribers_collection.count_documents({})
            total_admins = self.admins_collection.count_documents({})
            total_templates = self.templates_collection.count_documents({})
            total_scheduled = self.scheduled_broadcasts_collection.count_documents({'status': 'pending'})
            pending_approvals = self.broadcast_approvals_collection.count_documents({'status': 'pending'})
            pending_signals = self.signal_suggestions_collection.count_documents({'status': 'pending'})

            return {
                'total_users': total_users,
                'subscribers': total_subscribers,
                'non_subscribers': total_users - total_subscribers,
                'admins': total_admins,
                'templates': total_templates,
                'scheduled_broadcasts': total_scheduled,
                'pending_approvals': pending_approvals,
                'pending_signals': pending_signals
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}

    # Admin Management Methods
    def add_admin(self, user_id: int, role: AdminRole, added_by: int):
        """Add an admin with role"""
        try:
            self.admins_collection.update_one(
                {'user_id': user_id},
                {
                    '$set': {
                        'user_id': user_id,
                        'role': role.value,
                        'added_by': added_by,
                        'added_at': time.time()
                    }
                },
                upsert=True
            )
            self.log_activity(added_by, 'add_admin', {'target_user': user_id, 'role': role.value})
            return True
        except Exception as e:
            logger.error(f"Error adding admin {user_id}: {e}")
            return False

    def remove_admin(self, user_id: int, removed_by: int):
        """Remove an admin"""
        try:
            result = self.admins_collection.delete_one({'user_id': user_id})
            if result.deleted_count > 0:
                self.log_activity(removed_by, 'remove_admin', {'target_user': user_id})
                return True
            return False
        except Exception as e:
            logger.error(f"Error removing admin {user_id}: {e}")
            return False

    def get_admin_role(self, user_id: int) -> Optional[AdminRole]:
        """Get admin role"""
        try:
            admin = self.admins_collection.find_one({'user_id': user_id})
            if admin:
                return AdminRole(admin['role'])
            return None
        except Exception as e:
            logger.error(f"Error getting admin role for {user_id}: {e}")
            return None

    def get_all_admins(self) -> List[Dict]:
        """Get all admins"""
        try:
            return list(self.admins_collection.find({}))
        except Exception as e:
            logger.error(f"Error getting all admins: {e}")
            return []

    def has_permission(self, user_id: int, permission: Permission) -> bool:
        """Check if user has permission"""
        role = self.get_admin_role(user_id)
        if not role:
            return False
        return permission in ROLE_PERMISSIONS.get(role, [])

    # Activity Logging
    def log_activity(self, user_id: int, action: str, details: Dict = None):
        """Log admin activity"""
        try:
            log_entry = {
                'user_id': user_id,
                'action': action,
                'details': details or {},
                'timestamp': time.time()
            }
            self.activity_logs_collection.insert_one(log_entry)
        except Exception as e:
            logger.error(f"Error logging activity: {e}")

    def get_activity_logs(self, limit: int = 50, user_id: int = None) -> List[Dict]:
        """Get activity logs"""
        try:
            query = {'user_id': user_id} if user_id else {}
            logs = self.activity_logs_collection.find(query).sort('timestamp', -1).limit(limit)
            return list(logs)
        except Exception as e:
            logger.error(f"Error getting activity logs: {e}")
            return []

    def get_admin_stats(self, user_id: int) -> Dict:
        """Get statistics for an admin"""
        try:
            total_broadcasts = self.activity_logs_collection.count_documents({
                'user_id': user_id,
                'action': 'broadcast_sent'
            })
            total_templates = self.templates_collection.count_documents({'created_by': user_id})
            total_scheduled = self.scheduled_broadcasts_collection.count_documents({
                'created_by': user_id
            })

            return {
                'broadcasts': total_broadcasts,
                'templates': total_templates,
                'scheduled': total_scheduled
            }
        except Exception as e:
            logger.error(f"Error getting admin stats: {e}")
            return {}

    # Broadcast Approval Methods
    def create_broadcast_approval(self, message_data: Dict, created_by: int,
                                  creator_name: str, target: str, scheduled: bool = False) -> str:
        """Create a broadcast approval request"""
        try:
            approval = {
                'message_data': message_data,
                'created_by': created_by,
                'creator_name': creator_name,
                'target': target,
                'scheduled': scheduled,
                'status': 'pending',
                'created_at': time.time()
            }
            result = self.broadcast_approvals_collection.insert_one(approval)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error creating broadcast approval: {e}")
            return None

    def get_pending_approvals(self) -> List[Dict]:
        """Get all pending approval requests"""
        try:
            approvals = self.broadcast_approvals_collection.find({'status': 'pending'}).sort('created_at', 1)
            return list(approvals)
        except Exception as e:
            logger.error(f"Error getting pending approvals: {e}")
            return []

    def get_approval_by_id(self, approval_id: str):
        """Get approval request by ID"""
        try:
            from bson.objectid import ObjectId
            return self.broadcast_approvals_collection.find_one({'_id': ObjectId(approval_id)})
        except Exception as e:
            logger.error(f"Error getting approval: {e}")
            return None

    def update_approval_status(self, approval_id: str, status: str, reviewed_by: int, reason: str = None):
        """Update approval status"""
        try:
            from bson.objectid import ObjectId
            update_data = {
                'status': status,
                'reviewed_by': reviewed_by,
                'reviewed_at': time.time()
            }
            if reason:
                update_data['rejection_reason'] = reason

            self.broadcast_approvals_collection.update_one(
                {'_id': ObjectId(approval_id)},
                {'$set': update_data}
            )
            self.log_activity(reviewed_by, f'broadcast_{status}', {'approval_id': approval_id})
            return True
        except Exception as e:
            logger.error(f"Error updating approval status: {e}")
            return False

    # Signal Suggestion Methods
    def create_signal_suggestion(self, message_data: Dict, suggested_by: int,
                                suggester_name: str) -> str:
        """Create a signal suggestion"""
        try:
            suggestion = {
                'message_data': message_data,
                'suggested_by': suggested_by,
                'suggester_name': suggester_name,
                'status': 'pending',
                'created_at': time.time()
            }
            result = self.signal_suggestions_collection.insert_one(suggestion)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error creating signal suggestion: {e}")
            return None

    def get_pending_suggestions(self) -> List[Dict]:
        """Get all pending signal suggestions"""
        try:
            suggestions = self.signal_suggestions_collection.find({'status': 'pending'}).sort('created_at', 1)
            return list(suggestions)
        except Exception as e:
            logger.error(f"Error getting pending suggestions: {e}")
            return []

    def get_suggestion_by_id(self, suggestion_id: str):
        """Get suggestion by ID"""
        try:
            from bson.objectid import ObjectId
            return self.signal_suggestions_collection.find_one({'_id': ObjectId(suggestion_id)})
        except Exception as e:
            logger.error(f"Error getting suggestion: {e}")
            return None

    def update_suggestion_status(self, suggestion_id: str, status: str, reviewed_by: int, reason: str = None):
        """Update suggestion status"""
        try:
            from bson.objectid import ObjectId
            update_data = {
                'status': status,
                'reviewed_by': reviewed_by,
                'reviewed_at': time.time()
            }
            if reason:
                update_data['rejection_reason'] = reason

            self.signal_suggestions_collection.update_one(
                {'_id': ObjectId(suggestion_id)},
                {'$set': update_data}
            )
            self.log_activity(reviewed_by, f'signal_{status}', {'suggestion_id': suggestion_id})
            return True
        except Exception as e:
            logger.error(f"Error updating suggestion status: {e}")
            return False

    # Template Management
    def save_template(self, name: str, message_data: Dict, category: str, created_by: int):
        """Save a message template"""
        try:
            template = {
                'name': name,
                'message_data': message_data,
                'category': category,
                'created_by': created_by,
                'created_at': time.time(),
                'usage_count': 0
            }
            result = self.templates_collection.insert_one(template)
            self.log_activity(created_by, 'create_template', {'template_name': name, 'category': category})
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error saving template: {e}")
            return None

    def get_template(self, template_id: str):
        """Get a template by ID"""
        try:
            from bson.objectid import ObjectId
            return self.templates_collection.find_one({'_id': ObjectId(template_id)})
        except Exception as e:
            logger.error(f"Error getting template: {e}")
            return None

    def get_all_templates(self, category: str = None) -> List[Dict]:
        """Get all templates, optionally filtered by category"""
        try:
            query = {'category': category} if category else {}
            templates = self.templates_collection.find(query).sort('created_at', -1)
            return list(templates)
        except Exception as e:
            logger.error(f"Error getting templates: {e}")
            return []

    def delete_template(self, template_id: str, deleted_by: int):
        """Delete a template"""
        try:
            from bson.objectid import ObjectId
            result = self.templates_collection.delete_one({'_id': ObjectId(template_id)})
            if result.deleted_count > 0:
                self.log_activity(deleted_by, 'delete_template', {'template_id': template_id})
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting template: {e}")
            return False

    def increment_template_usage(self, template_id: str):
        """Increment template usage count"""
        try:
            from bson.objectid import ObjectId
            self.templates_collection.update_one(
                {'_id': ObjectId(template_id)},
                {'$inc': {'usage_count': 1}}
            )
        except Exception as e:
            logger.error(f"Error incrementing template usage: {e}")

    # Scheduled Broadcasts
    def schedule_broadcast(self, message_data: Dict, scheduled_time: float,
                          repeat: str, created_by: int, target: str):
        """Schedule a broadcast"""
        try:
            scheduled = {
                'message_data': message_data,
                'scheduled_time': scheduled_time,
                'repeat': repeat,
                'created_by': created_by,
                'target': target,
                'status': 'pending',
                'created_at': time.time()
            }
            result = self.scheduled_broadcasts_collection.insert_one(scheduled)
            self.log_activity(created_by, 'schedule_broadcast', {
                'scheduled_time': datetime.fromtimestamp(scheduled_time).isoformat(),
                'repeat': repeat
            })
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error scheduling broadcast: {e}")
            return None

    def get_pending_broadcasts(self) -> List[Dict]:
        """Get broadcasts that are ready to be sent"""
        try:
            current_time = time.time()
            broadcasts = self.scheduled_broadcasts_collection.find({
                'status': 'pending',
                'scheduled_time': {'$lte': current_time}
            })
            return list(broadcasts)
        except Exception as e:
            logger.error(f"Error getting pending broadcasts: {e}")
            return []

    def update_broadcast_status(self, broadcast_id: str, status: str):
        """Update broadcast status"""
        try:
            from bson.objectid import ObjectId
            self.scheduled_broadcasts_collection.update_one(
                {'_id': ObjectId(broadcast_id)},
                {'$set': {'status': status, 'executed_at': time.time()}}
            )
        except Exception as e:
            logger.error(f"Error updating broadcast status: {e}")

    def get_scheduled_broadcasts(self, created_by: int = None) -> List[Dict]:
        """Get all scheduled broadcasts"""
        try:
            query = {'created_by': created_by, 'status': 'pending'} if created_by else {'status': 'pending'}
            broadcasts = self.scheduled_broadcasts_collection.find(query).sort('scheduled_time', 1)
            return list(broadcasts)
        except Exception as e:
            logger.error(f"Error getting scheduled broadcasts: {e}")
            return []

    def cancel_scheduled_broadcast(self, broadcast_id: str, cancelled_by: int):
        """Cancel a scheduled broadcast"""
        try:
            from bson.objectid import ObjectId
            result = self.scheduled_broadcasts_collection.update_one(
                {'_id': ObjectId(broadcast_id)},
                {'$set': {'status': 'cancelled', 'cancelled_by': cancelled_by, 'cancelled_at': time.time()}}
            )
            if result.modified_count > 0:
                self.log_activity(cancelled_by, 'cancel_scheduled_broadcast', {'broadcast_id': broadcast_id})
                return True
            return False
        except Exception as e:
            logger.error(f"Error cancelling scheduled broadcast: {e}")
            return False

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


class ImageWatermarker:
    """Handle image watermarking"""

    @staticmethod
    def add_watermark(image_bytes: bytes, watermark_text: str = "PipSage") -> bytes:
        """Add watermark to image"""
        try:
            image = Image.open(io.BytesIO(image_bytes))
            if image.mode != 'RGB':
                image = image.convert('RGB')

            draw = ImageDraw.Draw(image)
            width, height = image.size
            font_size = int(min(width, height) * 0.05)

            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
            except:
                font = ImageFont.load_default()

            bbox = draw.textbbox((0, 0), watermark_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            padding = int(min(width, height) * 0.02)
            x = width - text_width - padding
            y = height - text_height - padding

            background_padding = int(font_size * 0.3)
            draw.rectangle(
                [x - background_padding, y - background_padding,
                 x + text_width + background_padding, y + text_height + background_padding],
                fill=(0, 0, 0, 180)
            )

            draw.text((x, y), watermark_text, fill=(255, 255, 255, 230), font=font)

            output = io.BytesIO()
            image.save(output, format='JPEG', quality=95)
            output.seek(0)

            return output.getvalue()
        except Exception as e:
            logger.error(f"Error adding watermark: {e}")
            return image_bytes


class BroadcastBot:
    def __init__(self, token: str, super_admin_ids: List[int], mongo_handler: MongoDBHandler):
        self.token = token
        self.super_admin_ids = super_admin_ids
        self.db = mongo_handler
        self.watermarker = ImageWatermarker()
        self.cr_numbers = {
            "CR5499637", "CR5500382", "CR5529877", "CR5535613", "CR5544922", "CR5551288",
            "CR5552176", "CR5556284", "CR5556287", "CR5561483", "CR5563616", "CR5577880",
            "CR5585327", "CR5589802", "CR5592846", "CR5594968", "CR5595416", "CR5597602",
            "CR5605478", "CR5607701", "CR5616548", "CR5616657", "CR5617024", "CR5618746",
            "CR5634872", "CR5638055", "CR5658165", "CR5662243", "CR5681280", "CR5686151",
            "CR5693620", "CR5694136", "CR5729218", "CR5729228", "CR5729255", "CR5734377",
            "CR5734685", "CR5734864", "CR5751222", "CR5755906", "CR5784782", "CR5786213",
            "CR5786969", "CR5799865", "CR5799868", "CR5799916", "CR5822964", "CR5836935",
            "CR5836938", "CR5839647", "CR5839797", "CR5859465", "CR5864046", "CR5873762",
            "CR5881030", "CR5886556", "CR5890102", "CR5924066", "CR5930200", "CR5970531",
            "CR6007156", "CR6012579", "CR6012919", "CR6022355", "CR6024318", "CR6037913",
            "CR6043787", "CR6077426", "CR6086720", "CR6094490", "CR6102922", "CR6128596",
            "CR6135793", "CR6141138", "CR6141427", "CR6141685", "CR6142172", "CR6142245",
            "CR6143176", "CR6146767", "CR6146888", "CR6167387", "CR6172824", "CR6181075",
            "CR6181076", "CR6182660", "CR6194673", "CR6198415", "CR6209246", "CR6268178",
            "CR6283228", "CR6295186", "CR6299453", "CR6301714", "CR6313536", "CR6316942",
            "CR6316943", "CR6316945", "CR6321295", "CR6330598", "CR6341042", "CR6379985",
            "CR6399552", "CR6401733", "CR6403902", "CR6413389", "CR6423099", "CR6423523",
            "CR6462778", "CR6474692", "CR6487699", "CR6505876", "CR6520436", "CR6520451",
            "CR6523858", "CR6524558", "CR6528520", "CR6532131", "CR6532137", "CR6532275",
            "CR6610101", "CR6620010", "CR6653814", "CR6667537", "CR6669363", "CR6669366",
            "CR6675564", "CR6676337", "CR6676341", "CR6682471", "CR6691842", "CR6691852",
            "CR6710741", "CR6756501", "CR6756521", "CR6762445", "CR6772496", "CR6799617",
            "CR6800730", "CR6973584", "CR6978912", "CR6983840", "CR6984178", "CR6994219",
            "CR7016028", "CR7044018", "CR7052204", "CR7112762", "CR7114951", "CR7124896",
            "CR7237163", "CR7310563", "CR7380411", "CR7381612", "CR5217806", "CR5218145",
            "CR5247338", "CR5431311", "CR5455669", "CR5141478", "CR5466762", "CR6154878",
            "CR6514641", "CR7443452", "CR7462159", "CR7496923", "CR7514165", "CR7619347",
            "CR7625010", "CR7655242", "CR7707424", "CR7708242", "CR4965219", "CR4985194",
            "CR5053549", "CR5085020", "CR5076079", "CR5115383", "CR5127519", "CR5128799",
            "CR5128821", "CR5128906", "CR5108974", "CR5140335", "CR5140339", "CR5146592",
            "CR5146651", "CR5140283", "CR5150548", "CR5168586", "CR5182098", "CR5195948",
            "CR5195953", "CR5195954", "CR5208742", "CR5191512", "CR5191516", "CR5230088",
            "CR5242731", "CR5232901", "CR5304118", "CR5376438", "CR5383018", "CR5559722",
            "CR5576367", "CR5583683", "CR5747075", "CR5845914", "CR5851342", "CR5851788",
            "CR5882107", "CR6174976", "CR6200366", "CR6156707", "CR6158587", "CR6300261",
            "CR6352212", "CR6384361", "CR6399574", "CR6408968", "CR6439217", "CR6706694",
            "CR6771489", "CR6828268", "CR7283876", "CR7283878", "CR7383923", "CR7383924",
            "CR7383926", "CR5107260", "CR5107344", "CR5121522", "CR5124042", "CR5131270",
            "CR5131273", "CR5140709", "CR5145112", "CR5145144", "CR5150792", "CR5151132",
            "CR5152411", "CR5156334", "CR5168665", "CR5171621", "CR5171935", "CR5172416",
            "CR5174518", "CR5175283", "CR5175357", "CR5175623", "CR5176885", "CR5178412",
            "CR5183689", "CR5192564", "CR5192768", "CR5196405", "CR5201751", "CR5201863",
            "CR5208818", "CR5209139", "CR5211727", "CR5217038", "CR5217041", "CR5217294",
            "CR5217716", "CR5217841", "CR5218709", "CR5220504", "CR5221257", "CR5222812",
            "CR5224492", "CR5234722", "CR5250590", "CR5253563", "CR5253566", "CR5253922",
            "CR5268275", "CR5273673", "CR5273869", "CR5276090", "CR5276310", "CR5281994",
            "CR5283490", "CR5283554", "CR5283705", "CR5283721", "CR5291732", "CR5298913",
            "CR5299111", "CR5299430", "CR5303230", "CR5304735", "CR5305240", "CR5305810",
            "CR5310002", "CR5317151", "CR5321069", "CR5324653", "CR5325581", "CR5327120",
            "CR5328157", "CR5337678", "CR5337712", "CR5337783", "CR5337784", "CR5337791",
            "CR5337793", "CR5404655", "CR5421490", "CR5442253", "CR5442355", "CR5442531",
            "CR5442605", "CR5444280", "CR5445094", "CR5446889", "CR5466632", "CR5471054",
            "CR5477031", "CR5485897", "CR5487026", "CR5487767", "CR5487928", "CR5488506",
            "CR5491460", "CR5499637", "CR5500382", "CR3648598", "CR3654244", "CR3654335",
            "CR3762108", "CR3845409", "CR3925151", "CR4085158", "CR4090372", "CR4138661",
            "CR4210749", "CR4296364", "CR4373296", "CR4488218", "CR4583558", "CR4655132",
            "CR4965219", "CR4985194", "CR5053549", "CR5085020", "CR5076079", "CR5115383",
            "CR5127519", "CR5128799", "CR5128821", "CR5128906", "CR7792475", "CR7814776",
            "CR7816651", "CR7817244", "CR7818330", "CR5149678", "CR8010847", "CR8036589",
            "CR8047034", "CR8052255", "CR7380411", "CR7707424", "CR8581785", "CR8644473",
            "CR8648274", "CR8661054",
        }
        # Initialize super admins in database
        for admin_id in super_admin_ids:
            self.db.add_admin(admin_id, AdminRole.SUPER_ADMIN, admin_id)

    def get_admin_role(self, user_id: int) -> Optional[AdminRole]:
        """Get user's admin role"""
        return self.db.get_admin_role(user_id)

    def is_admin(self, user_id: int) -> bool:
        """Check if user is any type of admin"""
        return self.get_admin_role(user_id) is not None

    def has_permission(self, user_id: int, permission: Permission) -> bool:
        """Check if user has specific permission"""
        return self.db.has_permission(user_id, permission)

    def needs_approval(self, user_id: int) -> bool:
        """Check if user's broadcasts need approval"""
        role = self.get_admin_role(user_id)
        # Broadcasters and Admins need approval, Super Admins and Moderators don't
        return role in [AdminRole.BROADCASTER, AdminRole.ADMIN]

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user = update.effective_user
        user_id = user.id

        self.db.add_user(user_id, user.username, user.first_name)

        if self.is_admin(user_id):
            role = self.get_admin_role(user_id)
            message = (
                f"🔧 Admin Panel ({role.value.replace('_', ' ').title()})\n\n"
                "📢 Broadcasting:\n"
                "/broadcast - Start broadcasting\n"
                "/schedule - Schedule a broadcast\n"
                "/scheduled - View scheduled broadcasts\n\n"
            )

            if self.has_permission(user_id, Permission.APPROVE_BROADCASTS):
                message += (
                    "✅ Approval System:\n"
                    "/approvals - View pending approvals\n"
                    "/signals - View signal suggestions\n\n"
                )

            message += (
                "📝 Templates:\n"
                "/templates - Manage templates\n"
                "/savetemplate - Save current as template\n\n"
                "👥 User Management:\n"
                "/add <user_id> - Add subscriber\n"
                "/stats - View statistics\n"
                "/subscribers - List subscribers\n\n"
            )

            if self.has_permission(user_id, Permission.MANAGE_ADMINS):
                message += (
                    "👨‍💼 Admin Management:\n"
                    "/addadmin - Add new admin\n"
                    "/removeadmin - Remove admin\n"
                    "/admins - List all admins\n\n"
                )

            if self.has_permission(user_id, Permission.VIEW_LOGS):
                message += (
                    "📊 Monitoring:\n"
                    "/logs - View activity logs\n"
                    "/mystats - Your statistics\n\n"
                )

            message += "/help - Show this message"
            await update.message.reply_text(message)
        else:
            message = (
                "👋 Welcome to PipSage — wise signals, steady gains!\n\n"
                "You'll get curated trade signals and VIP updates here.\n"
                "Enable notifications to get notified of broadcasts.\n\n"
                "💡 Commands:\n"
                "/subscribe - Join our VIP channels\n"
                "/suggestsignal - Suggest a trading signal\n"
                "/help - Show this message"
            )
            await update.message.reply_text(message)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        await self.start(update, context)

    async def unsubscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /unsubscribe command"""
        user_id = update.effective_user.id

        if self.db.remove_subscriber(user_id):
            await update.message.reply_text("🔕 Successfully unsubscribed from broadcasts!")
        else:
            await update.message.reply_text("❌ You're not currently subscribed.")

    async def add_subscriber_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /add command - Super Admin only"""
        if update.effective_user.id not in self.super_admin_ids:
            await update.message.reply_text("❌ Only Super Admins can add subscribers.")
            return

        if not context.args:
            await update.message.reply_text("❌ Please provide a user ID: /add <user_id>")
            return

        try:
            user_id = int(context.args[0])
            self.db.add_user(user_id)
            self.db.add_subscriber(user_id)
            self.db.log_activity(update.effective_user.id, 'manual_add_subscriber', {'user_id': user_id})
            await update.message.reply_text(f"✅ User {user_id} added to subscribers list!")
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. Please provide a numeric ID.")

    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command"""
        if not self.has_permission(update.effective_user.id, Permission.VIEW_STATS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        stats = self.db.get_stats()
        stats_text = (
            f"📊 Bot Statistics\n\n"
            f"👥 Total Users: {stats.get('total_users', 0)}\n"
            f"🔔 Subscribers: {stats.get('subscribers', 0)}\n"
            f"🔕 Non-subscribers: {stats.get('non_subscribers', 0)}\n"
            f"👨‍💼 Admins: {stats.get('admins', 0)}\n"
            f"📝 Templates: {stats.get('templates', 0)}\n"
            f"⏰ Scheduled: {stats.get('scheduled_broadcasts', 0)}\n"
            f"⏳ Pending Approvals: {stats.get('pending_approvals', 0)}\n"
            f"💡 Signal Suggestions: {stats.get('pending_signals', 0)}"
        )
        await update.message.reply_text(stats_text)
        self.db.log_activity(update.effective_user.id, 'view_stats', {})

    async def list_subscribers(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribers command"""
        if not self.has_permission(update.effective_user.id, Permission.VIEW_STATS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        subscribers = self.db.get_all_subscribers()

        if not subscribers:
            await update.message.reply_text("📝 No subscribers yet.")
            return

        subscribers_list = "\n".join([f"• {sub_id}" for sub_id in sorted(subscribers)])
        message = f"📝 Subscribers List ({len(subscribers)} total):\n\n{subscribers_list}"

        if len(message) > 4000:
            chunks = [subscribers_list[i:i+3500] for i in range(0, len(subscribers_list), 3500)]
            await update.message.reply_text(f"📝 Subscribers List ({len(subscribers)} total):")
            for chunk in chunks:
                await update.message.reply_text(chunk)
        else:
            await update.message.reply_text(message)

    # Signal Suggestion (available to all users)
    async def suggest_signal_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start signal suggestion conversation"""
        await update.message.reply_text(
            "💡 Suggest a Trading Signal\n\n"
            "Please send me your signal suggestion.\n"
            "You can send text, photos, or documents.\n\n"
            "Your suggestion will be reviewed by Super Admins.\n\n"
            "Send /cancel to cancel."
        )
        return WAITING_SIGNAL_MESSAGE

    async def receive_signal_suggestion(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive signal suggestion message"""
        user = update.effective_user
        message = update.message

        # Prepare message data
        message_data = {
            'type': 'text',
            'content': None
        }

        if message.text:
            message_data['type'] = 'text'
            message_data['content'] = message.text
        elif message.photo:
            message_data['type'] = 'photo'
            message_data['file_id'] = message.photo[-1].file_id
            message_data['caption'] = message.caption
        elif message.video:
            message_data['type'] = 'video'
            message_data['file_id'] = message.video.file_id
            message_data['caption'] = message.caption
        elif message.document:
            message_data['type'] = 'document'
            message_data['file_id'] = message.document.file_id
            message_data['caption'] = message.caption

        # Save suggestion
        suggestion_id = self.db.create_signal_suggestion(
            message_data,
            user.id,
            user.first_name or user.username or str(user.id)
        )

        if suggestion_id:
            await update.message.reply_text(
                "✅ Signal suggestion submitted!\n\n"
                "Super Admins will review your suggestion.\n"
                "You'll be notified when it's reviewed."
            )

            # Notify super admins
            await self.notify_super_admins_new_suggestion(context, suggestion_id)
        else:
            await update.message.reply_text("❌ Failed to submit suggestion. Please try again.")

        return ConversationHandler.END

    async def notify_super_admins_new_suggestion(self, context: ContextTypes.DEFAULT_TYPE, suggestion_id: str):
        """Notify super admins of new signal suggestion"""
        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if not suggestion:
            return

        short_id = str(suggestion['_id'])[-8:]
        suggester = suggestion['suggester_name']

        notification = (
            f"💡 New Signal Suggestion!\n\n"
            f"From: {suggester}\n"
            f"ID: {short_id}\n\n"
            f"Use /signals to review pending suggestions."
        )

        for admin_id in self.super_admin_ids:
            try:
                await context.bot.send_message(chat_id=admin_id, text=notification)
            except Exception as e:
                logger.error(f"Failed to notify super admin {admin_id}: {e}")

    async def list_signal_suggestions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /signals command"""
        if update.effective_user.id not in self.super_admin_ids:
            await update.message.reply_text("❌ Only Super Admins can review signal suggestions.")
            return

        suggestions = self.db.get_pending_suggestions()

        if not suggestions:
            await update.message.reply_text("💡 No pending signal suggestions.")
            return

        await update.message.reply_text(f"💡 {len(suggestions)} Pending Signal Suggestion(s):\n\nReviewing...")

        for suggestion in suggestions:
            await self.show_signal_suggestion(update, context, suggestion)

    async def show_signal_suggestion(self, update: Update, context: ContextTypes.DEFAULT_TYPE, suggestion: Dict):
        """Show a signal suggestion for review"""
        suggestion_id = str(suggestion['_id'])
        short_id = suggestion_id[-8:]
        suggester = suggestion['suggester_name']
        created_at = datetime.fromtimestamp(suggestion['created_at']).strftime('%Y-%m-%d %H:%M')

        message_data = suggestion['message_data']

        keyboard = [
            [
                InlineKeyboardButton("✅ Approve & Broadcast", callback_data=f"sig_approve_{suggestion_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"sig_reject_{suggestion_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        header = (
            f"💡 Signal Suggestion\n"
            f"ID: {short_id}\n"
            f"From: {suggester}\n"
            f"Submitted: {created_at}\n"
            f"{'─' * 30}\n\n"
        )

        try:
            if message_data['type'] == 'text':
                full_message = header + message_data['content']
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=full_message,
                    reply_markup=reply_markup
                )
            elif message_data['type'] == 'photo':
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=message_data['file_id'],
                    caption=header + (message_data.get('caption') or ''),
                    reply_markup=reply_markup
                )
            elif message_data['type'] == 'video':
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=message_data['file_id'],
                    caption=header + (message_data.get('caption') or ''),
                    reply_markup=reply_markup
                )
            elif message_data['type'] == 'document':
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=message_data['file_id'],
                    caption=header + (message_data.get('caption') or ''),
                    reply_markup=reply_markup
                )
        except Exception as e:
            logger.error(f"Error showing signal suggestion: {e}")

    async def handle_signal_review(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle signal suggestion approval/rejection"""
        query = update.callback_query
        await query.answer()

        if query.from_user.id not in self.super_admin_ids:
            await query.edit_message_text("❌ Only Super Admins can review suggestions.")
            return

        action, suggestion_id = query.data.split('_', 2)[1:]

        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if not suggestion:
            await query.edit_message_text("❌ Suggestion not found.")
            return

        if action == "approve":
            # Update status
            self.db.update_suggestion_status(suggestion_id, 'approved', query.from_user.id)

            # Broadcast to all users
            await self.broadcast_signal(context, suggestion)

            # Notify suggester
            try:
                await context.bot.send_message(
                    chat_id=suggestion['suggested_by'],
                    text="✅ Your signal suggestion has been approved and broadcasted! Thank you for your contribution."
                )
            except:
                pass

            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text("✅ Signal approved and broadcasted to all users!")

        elif action == "reject":
            self.db.update_suggestion_status(suggestion_id, 'rejected', query.from_user.id)

            # Notify suggester
            try:
                await context.bot.send_message(
                    chat_id=suggestion['suggested_by'],
                    text="❌ Your signal suggestion was not approved at this time. Thank you for your submission."
                )
            except:
                pass

            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text("❌ Signal rejected.")

    async def broadcast_signal(self, context: ContextTypes.DEFAULT_TYPE, suggestion: Dict):
        """Broadcast approved signal to all users"""
        target_users = self.db.get_all_users()
        message_data = suggestion['message_data']
        suggester = suggestion['suggester_name']

        # Add attribution to message
        attribution = f"\n\n💡 Signal suggested by: {suggester}"

        success_count = 0
        failed_count = 0

        for user_id in target_users:
            try:
                if message_data['type'] == 'text':
                    full_text = message_data['content'] + attribution
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=full_text
                    )
                elif message_data['type'] == 'photo':
                    caption = (message_data.get('caption') or '') + attribution
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=message_data['file_id'],
                        caption=caption
                    )
                elif message_data['type'] == 'video':
                    caption = (message_data.get('caption') or '') + attribution
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=message_data['file_id'],
                        caption=caption
                    )
                elif message_data['type'] == 'document':
                    caption = (message_data.get('caption') or '') + attribution
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=message_data['file_id'],
                        caption=caption
                    )

                success_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send signal to {user_id}: {e}")
                failed_count += 1

        logger.info(f"Signal broadcast completed: {success_count} success, {failed_count} failed")

    # Approval System
    async def list_approvals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /approvals command"""
        if not self.has_permission(update.effective_user.id, Permission.APPROVE_BROADCASTS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        approvals = self.db.get_pending_approvals()

        if not approvals:
            await update.message.reply_text("✅ No pending broadcast approvals.")
            return

        await update.message.reply_text(f"⏳ {len(approvals)} Pending Broadcast(s):\n\nReviewing...")

        for approval in approvals:
            await self.show_approval_request(update, context, approval)

    async def show_approval_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE, approval: Dict):
        """Show an approval request"""
        approval_id = str(approval['_id'])
        short_id = approval_id[-8:]
        creator = approval['creator_name']
        target = approval['target'].title()
        created_at = datetime.fromtimestamp(approval['created_at']).strftime('%Y-%m-%d %H:%M')

        message_data = approval['message_data']

        keyboard = [
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"app_approve_{approval_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"app_reject_{approval_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        header = (
            f"📢 Broadcast Approval Request\n"
            f"ID: {short_id}\n"
            f"Creator: {creator}\n"
            f"Target: {target}\n"
            f"Created: {created_at}\n"
            f"{'─' * 30}\n\n"
        )

        try:
            if message_data['type'] == 'text':
                full_message = header + message_data['content']
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=full_message,
                    reply_markup=reply_markup
                )
            elif message_data['type'] == 'photo':
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=message_data['file_id'],
                    caption=header + (message_data.get('caption') or ''),
                    reply_markup=reply_markup
                )
            elif message_data['type'] == 'video':
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=message_data['file_id'],
                    caption=header + (message_data.get('caption') or ''),
                    reply_markup=reply_markup
                )
            elif message_data['type'] == 'document':
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=message_data['file_id'],
                    caption=header + (message_data.get('caption') or ''),
                    reply_markup=reply_markup
                )
        except Exception as e:
            logger.error(f"Error showing approval request: {e}")

    async def handle_approval_review(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle broadcast approval/rejection"""
        query = update.callback_query
        await query.answer()

        if not self.has_permission(query.from_user.id, Permission.APPROVE_BROADCASTS):
            await query.edit_message_text("❌ You don't have permission to approve broadcasts.")
            return

        action, approval_id = query.data.split('_', 2)[1:]

        approval = self.db.get_approval_by_id(approval_id)
        if not approval:
            await query.edit_message_text("❌ Approval request not found.")
            return

        if action == "approve":
            # Update status
            self.db.update_approval_status(approval_id, 'approved', query.from_user.id)

            # Execute broadcast
            await self.execute_approved_broadcast(context, approval, query.from_user.id)

            # Notify creator
            try:
                await context.bot.send_message(
                    chat_id=approval['created_by'],
                    text="✅ Your broadcast has been approved and sent!"
                )
            except:
                pass

            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text("✅ Broadcast approved and sent!")

        elif action == "reject":
            self.db.update_approval_status(approval_id, 'rejected', query.from_user.id)

            # Notify creator
            try:
                await context.bot.send_message(
                    chat_id=approval['created_by'],
                    text="❌ Your broadcast was rejected."
                )
            except:
                pass

            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text("❌ Broadcast rejected.")

    async def execute_approved_broadcast(self, context: ContextTypes.DEFAULT_TYPE,
                                        approval: Dict, approved_by: int):
        """Execute an approved broadcast"""
        message_data = approval['message_data']
        target = approval['target']

        # Get target users
        all_users = self.db.get_all_users()
        subscribers = self.db.get_all_subscribers()
        admin_ids = self.db.get_all_admin_ids()

        if target == 'all':
            target_users = all_users
        elif target == 'subscribers':
            target_users = subscribers
        elif target == 'nonsubscribers':
            target_users = all_users - subscribers
        elif target == 'admins':
            target_users = admin_ids
        else:
            target_users = all_users

        success_count = 0
        failed_count = 0

        for user_id in target_users:
            try:
                if message_data['type'] == 'text':
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=message_data['content'],
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )
                elif message_data['type'] == 'photo':
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=message_data['file_id'],
                        caption=message_data.get('caption'),
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )
                elif message_data['type'] == 'video':
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=message_data['file_id'],
                        caption=message_data.get('caption'),
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )
                elif message_data['type'] == 'document':
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=message_data['file_id'],
                        caption=message_data.get('caption'),
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )

                success_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send approved broadcast to {user_id}: {e}")
                failed_count += 1

        # Log the broadcast
        self.db.log_activity(approved_by, 'approved_broadcast_sent', {
            'approval_id': str(approval['_id']),
            'creator': approval['created_by'],
            'target': target,
            'success': success_count,
            'failed': failed_count
        })

        logger.info(f"Approved broadcast sent: {success_count} success, {failed_count} failed")
        
    async def start_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start broadcast conversation"""
        if not self.has_permission(update.effective_user.id, Permission.BROADCAST):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END

        await update.message.reply_text(
            "📢 Start Broadcasting\n\n"
            "Send me the message to broadcast.\n"
            "You can send text, photos, videos, or documents.\n\n"
            "Send /cancel to cancel."
        )
        context.user_data.clear()
        return WAITING_MESSAGE

    async def schedule_broadcast_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start scheduled broadcast conversation"""
        if not self.has_permission(update.effective_user.id, Permission.SCHEDULE_BROADCASTS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END

        await update.message.reply_text(
            "🗓️ Schedule a Broadcast\n\n"
            "First, send me the message to schedule.\n"
            "You can send text, photos, videos, or documents.\n\n"
            "Send /cancel to cancel."
        )
        context.user_data.clear()
        return WAITING_MESSAGE

    async def receive_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive broadcast message and ask for next step"""
        context.user_data['broadcast_message'] = update.message

        if update.message.photo:
            keyboard = [
                [
                    InlineKeyboardButton("💧 Add Watermark", callback_data="watermark_yes"),
                    InlineKeyboardButton("➡️ Skip", callback_data="watermark_no")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Add a watermark to the image?", reply_markup=reply_markup)
            return WAITING_BUTTONS

        keyboard = [
            [
                InlineKeyboardButton("➕ Add Buttons", callback_data="add_buttons"),
                InlineKeyboardButton("➡️ Skip", callback_data="skip_buttons")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Do you want to add inline buttons to your message?",
                                      reply_markup=reply_markup)
        return WAITING_BUTTONS
        
    async def handle_watermark_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle watermark choice"""
        query = update.callback_query
        await query.answer()

        use_watermark = query.data == "watermark_yes"
        context.user_data['use_watermark'] = use_watermark

        if use_watermark:
            await query.edit_message_text("Watermarking image...")
            message = context.user_data['broadcast_message']
            photo_file = await message.photo[-1].get_file()
            
            image_bytes = await photo_file.download_as_bytearray()
            watermarked_image = self.watermarker.add_watermark(bytes(image_bytes))
            
            context.user_data['watermarked_image'] = watermarked_image

        keyboard = [
            [
                InlineKeyboardButton("➕ Add Buttons", callback_data="add_buttons"),
                InlineKeyboardButton("➡️ Skip", callback_data="skip_buttons")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("Do you want to add inline buttons to your message?",
                                      reply_markup=reply_markup)
        return WAITING_BUTTONS
        
    async def handle_buttons_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle buttons choice"""
        query = update.callback_query
        await query.answer()

        if query.data == "add_buttons":
            await query.edit_message_text(
                "Send me the buttons in the format:\n"
                "Button 1 text | http://example.com/link1\n"
                "Button 2 text | http://example.com/link2"
            )
            return WAITING_BUTTONS
        else:
            context.user_data['inline_buttons'] = None
            keyboard = [
                [
                    InlineKeyboardButton("🔒 Protect Content", callback_data="protect_yes"),
                    InlineKeyboardButton("🔓 Don't Protect", callback_data="protect_no")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Do you want to protect the message content?",
                                          reply_markup=reply_markup)
            return WAITING_PROTECTION
            
    async def receive_buttons(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive and parse buttons"""
        lines = update.message.text.strip().split('\n')
        buttons = []
        for line in lines:
            parts = line.split('|', 1)
            if len(parts) == 2:
                text = parts[0].strip()
                url = parts[1].strip()
                buttons.append([InlineKeyboardButton(text, url=url)])

        context.user_data['inline_buttons'] = InlineKeyboardMarkup(buttons)
        
        keyboard = [
            [
                InlineKeyboardButton("🔒 Protect Content", callback_data="protect_yes"),
                InlineKeyboardButton("🔓 Don't Protect", callback_data="protect_no")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Do you want to protect the message content?",
                                      reply_markup=reply_markup)
        return WAITING_PROTECTION

    async def handle_protection_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle content protection choice"""
        query = update.callback_query
        await query.answer()

        protect_content = query.data == "protect_yes"
        context.user_data['protect_content'] = protect_content

        # If this is a scheduled broadcast, ask for time
        if 'scheduled' in context.user_data and context.user_data['scheduled']:
            await query.edit_message_text(
                "⏰ Enter scheduled time (e.g., '2024-12-31 23:59') or relative time (e.g., '1h 30m')."
            )
            return WAITING_SCHEDULE_TIME

        return await self.ask_target_audience(query, context, scheduled=False)
        
    async def ask_target_audience(self, update, context: ContextTypes.DEFAULT_TYPE, scheduled=False):
        """Ask who to send the broadcast to"""
        stats = self.db.get_stats()

        keyboard = [
            [InlineKeyboardButton("👥 All Users", callback_data="target_all")],
            [InlineKeyboardButton("🔔 Subscribers Only", callback_data="target_subscribers")],
            [InlineKeyboardButton("🔕 Non-subscribers Only", callback_data="target_nonsubscribers")],
            [InlineKeyboardButton("👨‍💼 Admins Only", callback_data="target_admins")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        stats_text = (
            f"👥 All Users: {stats['total_users']}\n"
            f"🔔 Subscribers: {stats['subscribers']}\n"
            f"🔕 Non-subscribers: {stats['non_subscribers']}\n"
            f"👨‍💼 Admins: {stats['admins']}"
        )

        message = f"🎯 Choose Target Audience\n\n{stats_text}\n\nWho should receive this broadcast?"

        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.message.reply_text(message, reply_markup=reply_markup)
        elif hasattr(update, 'message'):
            await update.message.reply_text(message, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=update.from_user.id, text=message, reply_markup=reply_markup)

        return WAITING_TARGET

    async def handle_target_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle target audience choice and send broadcast"""
        query = update.callback_query
        await query.answer()

        # Check if this is a scheduled broadcast
        if 'scheduled_time' in context.user_data:
            return await self.finalize_scheduled_broadcast(update, context)

        # Get target
        target_map = {
            'target_all': 'all',
            'target_subscribers': 'subscribers',
            'target_nonsubscribers': 'nonsubscribers',
            'target_admins': 'admins'
        }
        target = target_map.get(query.data, 'all')

        # Check if broadcast needs approval
        if self.needs_approval(query.from_user.id):
            # Create approval request
            broadcast_message = context.user_data['broadcast_message']
            inline_buttons = context.user_data.get('inline_buttons')
            protect_content = context.user_data.get('protect_content', False)
            use_watermark = context.user_data.get('use_watermark', False)

            # Prepare message data
            message_data = {
                'type': 'text',
                'content': None,
                'inline_buttons': inline_buttons,
                'protect_content': protect_content
            }

            if broadcast_message.text:
                message_data['type'] = 'text'
                message_data['content'] = broadcast_message.text
            elif broadcast_message.photo:
                message_data['type'] = 'photo'
                if use_watermark and 'watermarked_image' in context.user_data:
                    # Store watermarked image reference
                    message_data['file_id'] = broadcast_message.photo[-1].file_id
                    message_data['is_watermarked'] = True
                else:
                    message_data['file_id'] = broadcast_message.photo[-1].file_id
                message_data['caption'] = broadcast_message.caption
            elif broadcast_message.video:
                message_data['type'] = 'video'
                message_data['file_id'] = broadcast_message.video.file_id
                message_data['caption'] = broadcast_message.caption
            elif broadcast_message.document:
                message_data['type'] = 'document'
                message_data['file_id'] = broadcast_message.document.file_id
                message_data['caption'] = broadcast_message.caption

            creator_name = query.from_user.first_name or query.from_user.username or str(query.from_user.id)
            approval_id = self.db.create_broadcast_approval(
                message_data,
                query.from_user.id,
                creator_name,
                target
            )

            if approval_id:
                await query.edit_message_text(
                    "⏳ Broadcast submitted for approval!\n\n"
                    "Moderators/Super Admins will review your broadcast.\n"
                    "You'll be notified when it's reviewed."
                )

                # Notify approvers
                await self.notify_approvers_new_broadcast(context, approval_id)
            else:
                await query.edit_message_text("❌ Failed to submit broadcast. Please try again.")

            return ConversationHandler.END

        # Direct broadcast (Super Admin or Moderator)
        all_users = self.db.get_all_users()
        subscribers = self.db.get_all_subscribers()
        admin_ids = self.db.get_all_admin_ids()

        if target == 'all':
            target_users = all_users
            audience_name = "All Users"
        elif target == 'subscribers':
            target_users = subscribers
            audience_name = "Subscribers"
        elif target == 'nonsubscribers':
            target_users = all_users - subscribers
            audience_name = "Non-subscribers"
        elif target == 'admins':
            target_users = admin_ids
            audience_name = "Admins"
        else:
            target_users = all_users
            audience_name = "All Users"

        if not target_users:
            await query.edit_message_text(f"❌ No {audience_name.lower()} found.")
            return ConversationHandler.END

        broadcast_message = context.user_data['broadcast_message']
        inline_buttons = context.user_data.get('inline_buttons')
        protect_content = context.user_data.get('protect_content', False)
        use_watermark = context.user_data.get('use_watermark', False)

        protection_status = "🔒 Protected" if protect_content else "🔓 Unprotected"
        watermark_status = "💧 Watermarked" if use_watermark else "No watermark"

        message = (
            f"📡 Broadcasting to {audience_name}\n\n"
            f"Target: {len(target_users)} users\n"
            f"Protection: {protection_status}\n"
            f"Watermark: {watermark_status}\n\n"
            f"Sending..."
        )
        await query.edit_message_text(message)

        success_count = 0
        failed_count = 0

        for user_id in target_users:
            try:
                if broadcast_message.text:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=broadcast_message.text,
                        reply_markup=inline_buttons,
                        protect_content=protect_content
                    )
                elif broadcast_message.photo:
                    if use_watermark and 'watermarked_image' in context.user_data:
                        await context.bot.send_photo(
                            chat_id=user_id,
                            photo=context.user_data['watermarked_image'],
                            caption=broadcast_message.caption,
                            reply_markup=inline_buttons,
                            protect_content=protect_content
                        )
                    else:
                        await context.bot.send_photo(
                            chat_id=user_id,
                            photo=broadcast_message.photo[-1].file_id,
                            caption=broadcast_message.caption,
                            reply_markup=inline_buttons,
                            protect_content=protect_content
                        )
                elif broadcast_message.video:
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=broadcast_message.video.file_id,
                        caption=broadcast_message.caption,
                        reply_markup=inline_buttons,
                        protect_content=protect_content
                    )
                elif broadcast_message.document:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=broadcast_message.document.file_id,
                        caption=broadcast_message.caption,
                        reply_markup=inline_buttons,
                        protect_content=protect_content
                    )

                success_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send to {user_id}: {e}")
                failed_count += 1

        # Log the broadcast
        self.db.log_activity(query.from_user.id, 'broadcast_sent', {
            'target': audience_name,
            'success': success_count,
            'failed': failed_count
        })

        summary = (
            f"✅ Broadcast Complete!\n\n"
            f"📊 Results:\n"
            f"Target: {audience_name}\n"
            f"Successfully sent: {success_count}\n"
            f"Failed: {failed_count}\n"
            f"Total: {len(target_users)}"
        )

        await context.bot.send_message(chat_id=query.from_user.id, text=summary)
        return ConversationHandler.END

    async def notify_approvers_new_broadcast(self, context: ContextTypes.DEFAULT_TYPE, approval_id: str):
        """Notify approvers of new broadcast pending approval"""
        approval = self.db.get_approval_by_id(approval_id)
        if not approval:
            return

        short_id = str(approval['_id'])[-8:]
        creator = approval['creator_name']

        notification = (
            f"⏳ New Broadcast Pending Approval!\n\n"
            f"Creator: {creator}\n"
            f"ID: {short_id}\n\n"
            f"Use /approvals to review pending broadcasts."
        )

        # Notify all users with approval permission
        admins = self.db.get_all_admins()
        for admin in admins:
            if self.has_permission(admin['user_id'], Permission.APPROVE_BROADCASTS):
                try:
                    await context.bot.send_message(chat_id=admin['user_id'], text=notification)
                except Exception as e:
                    logger.error(f"Failed to notify approver {admin['user_id']}: {e}")

    async def cancel_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel broadcast operation"""
        await update.message.reply_text("❌ Operation cancelled.")
        return ConversationHandler.END

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle errors"""
        logger.error(f"Update {update} caused error {context.error}")

        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ An error occurred. Please try again."
                )
            except Exception:
                pass

    async def process_scheduled_broadcasts(self, context: ContextTypes.DEFAULT_TYPE):
        """Process scheduled broadcasts (runs periodically)"""
        try:
            pending = self.db.get_pending_broadcasts()

            for broadcast in pending:
                try:
                    message_data = broadcast['message_data']
                    target = broadcast['target']
                    broadcast_id = str(broadcast['_id'])

                    # Get target users
                    all_users = self.db.get_all_users()
                    subscribers = self.db.get_all_subscribers()
                    admin_ids = self.db.get_all_admin_ids()

                    if target == 'all':
                        target_users = all_users
                    elif target == 'subscribers':
                        target_users = subscribers
                    elif target == 'nonsubscribers':
                        target_users = all_users - subscribers
                    elif target == 'admins':
                        target_users = admin_ids
                    else:
                        target_users = all_users

                    success_count = 0
                    failed_count = 0

                    # Send messages
                    for user_id in target_users:
                        try:
                            if message_data['type'] == 'text':
                                await context.bot.send_message(
                                    chat_id=user_id,
                                    text=message_data['content'],
                                    reply_markup=message_data.get('inline_buttons'),
                                    protect_content=message_data.get('protect_content', False)
                                )
                            elif message_data['type'] == 'photo':
                                await context.bot.send_photo(
                                    chat_id=user_id,
                                    photo=message_data['file_id'],
                                    caption=message_data.get('caption'),
                                    reply_markup=message_data.get('inline_buttons'),
                                    protect_content=message_data.get('protect_content', False)
                                )
                            elif message_data['type'] == 'video':
                                await context.bot.send_video(
                                    chat_id=user_id,
                                    video=message_data['file_id'],
                                    caption=message_data.get('caption'),
                                    reply_markup=message_data.get('inline_buttons'),
                                    protect_content=message_data.get('protect_content', False)
                                )
                            elif message_data['type'] == 'document':
                                await context.bot.send_document(
                                    chat_id=user_id,
                                    document=message_data['file_id'],
                                    caption=message_data.get('caption'),
                                    reply_markup=message_data.get('inline_buttons'),
                                    protect_content=message_data.get('protect_content', False)
                                )

                            success_count += 1
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            logger.error(f"Failed to send scheduled to {user_id}: {e}")
                            failed_count += 1

                    # Update status
                    if broadcast['repeat'] == 'once':
                        self.db.update_broadcast_status(broadcast_id, 'completed')
                    else:
                        # Reschedule
                        next_time = self.calculate_next_time(broadcast['scheduled_time'], broadcast['repeat'])
                        self.db.scheduled_broadcasts_collection.update_one(
                            {'_id': broadcast['_id']},
                            {'$set': {'scheduled_time': next_time}}
                        )

                    # Log activity
                    self.db.log_activity(broadcast['created_by'], 'scheduled_broadcast_sent', {
                        'broadcast_id': broadcast_id,
                        'success': success_count,
                        'failed': failed_count
                    })

                    logger.info(f"Scheduled broadcast {broadcast_id} completed: {success_count}/{success_count + failed_count}")

                except Exception as e:
                    logger.error(f"Error processing scheduled broadcast: {e}")

        except Exception as e:
            logger.error(f"Error in process_scheduled_broadcasts: {e}")

    def calculate_next_time(self, current_time: float, repeat: str) -> float:
        """Calculate next scheduled time based on repeat pattern"""
        dt = datetime.fromtimestamp(current_time)

        if repeat == 'daily':
            next_dt = dt + timedelta(days=1)
        elif repeat == 'weekly':
            next_dt = dt + timedelta(weeks=1)
        elif repeat == 'monthly':
            next_dt = dt + timedelta(days=30)
        else:
            next_dt = dt

        return next_dt.timestamp()

    def create_health_server(self, port: int):
        """Create health check server"""
        async def health_check(request):
            return web.Response(text="OK", status=200)

        async def root_handler(request):
            return web.Response(text="Telegram Bot Running", status=200)

        app = web.Application()
        app.router.add_get('/health', health_check)
        app.router.add_get('/', root_handler)
        return app

    def run_health_server(self, port: int):
        """Run health check server in thread"""
        async def start_server():
            app = self.create_health_server(port)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', port)
            await site.start()
            logger.info(f"Health server on port {port}")

            while True:
                await asyncio.sleep(1)

        def run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(start_server())

        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()

    def create_application(self):
        """Create and configure application"""
        application = Application.builder().token(self.token).build()

        # Broadcast handler
        broadcast_handler = ConversationHandler(
            entry_points=[CommandHandler("broadcast", self.start_broadcast)],
            states={
                WAITING_MESSAGE: [
                    MessageHandler(filters.ALL & ~filters.COMMAND, self.receive_broadcast_message)
                ],
                WAITING_BUTTONS: [
                    CallbackQueryHandler(self.handle_watermark_choice, pattern="^watermark_"),
                    CallbackQueryHandler(self.handle_buttons_choice, pattern="^(add_buttons|skip_buttons)$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_buttons)
                ],
                WAITING_PROTECTION: [
                    CallbackQueryHandler(self.handle_protection_choice, pattern="^protect_")
                ],
                WAITING_TARGET: [
                    CallbackQueryHandler(self.handle_target_choice, pattern="^target_")
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

        # Schedule broadcast handler
        schedule_handler = ConversationHandler(
            entry_points=[CommandHandler("schedule", self.schedule_broadcast_start)],
            states={
                WAITING_MESSAGE: [
                    MessageHandler(filters.ALL & ~filters.COMMAND, self.receive_broadcast_message)
                ],
                WAITING_BUTTONS: [
                    CallbackQueryHandler(self.handle_watermark_choice, pattern="^watermark_"),
                    CallbackQueryHandler(self.handle_buttons_choice, pattern="^(add_buttons|skip_buttons)$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_buttons)
                ],
                WAITING_PROTECTION: [
                    CallbackQueryHandler(self.handle_protection_choice, pattern="^protect_")
                ],
                WAITING_SCHEDULE_TIME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_schedule_time)
                ],
                WAITING_SCHEDULE_REPEAT: [
                    CallbackQueryHandler(self.receive_schedule_repeat, pattern="^repeat_")
                ],
                WAITING_TARGET: [
                    CallbackQueryHandler(self.handle_target_choice, pattern="^target_")
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

        # Template save handler
        template_handler = ConversationHandler(
            entry_points=[CommandHandler("savetemplate", self.save_template_start)],
            states={
                WAITING_TEMPLATE_MESSAGE: [
                    MessageHandler(filters.ALL & ~filters.COMMAND, self.receive_template_message)
                ],
                WAITING_TEMPLATE_NAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_template_name)
                ],
                WAITING_TEMPLATE_CATEGORY: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_template_category)
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

        # Add admin handler
        add_admin_handler = ConversationHandler(
            entry_points=[CommandHandler("addadmin", self.add_admin_start)],
            states={
                WAITING_ADMIN_ID: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_admin_id)
                ],
                WAITING_ADMIN_ROLE: [
                    CallbackQueryHandler(self.receive_admin_role, pattern="^role_")
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

        # Signal suggestion handler
        signal_handler = ConversationHandler(
            entry_points=[CommandHandler("suggestsignal", self.suggest_signal_start)],
            states={
                WAITING_SIGNAL_MESSAGE: [
                    MessageHandler(filters.ALL & ~filters.COMMAND, self.receive_signal_suggestion)
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

        # New subscribe handler
        subscribe_handler = ConversationHandler(
            entry_points=[CommandHandler("subscribe", self.subscribe_start)],
            states={
                WAITING_VIP_GROUP: [CallbackQueryHandler(self.receive_vip_group)],
                WAITING_ACCOUNT_CREATION_CONFIRMATION: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_account_creation_confirmation)
                ],
                WAITING_ACCOUNT_DATE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_account_date)
                ],
                WAITING_CR_NUMBER: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_cr_number)
                ],
                WAITING_SCREENSHOT: [
                    MessageHandler(filters.PHOTO, self.receive_screenshot)
                ],
                WAITING_KENNEDYNESPOT_CONFIRMATION: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_kennedynespot_confirmation)
                ],
                WAITING_BROKER_CHOICE: [
                    CallbackQueryHandler(self.receive_broker_choice, pattern="^broker_")
                ],
                WAITING_ACCOUNT_NAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_account_name)
                ],
                WAITING_ACCOUNT_NUMBER: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_account_number)
                ],
                WAITING_TELEGRAM_ID: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_telegram_id)
                ],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)],
        )

        # Basic commands
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help_command))
        application.add_handler(CommandHandler("unsubscribe", self.unsubscribe))
        application.add_handler(CommandHandler("add", self.add_subscriber_command))
        application.add_handler(CommandHandler("stats", self.stats))
        application.add_handler(CommandHandler("subscribers", self.list_subscribers))

        # Approval system
        application.add_handler(CommandHandler("approvals", self.list_approvals))
        application.add_handler(CommandHandler("signals", self.list_signal_suggestions))
        application.add_handler(CallbackQueryHandler(self.handle_approval_review, pattern="^app_"))
        application.add_handler(CallbackQueryHandler(self.handle_signal_review, pattern="^sig_"))

        # Admin management
        application.add_handler(add_admin_handler)
        application.add_handler(CommandHandler("removeadmin", self.remove_admin_command))
        application.add_handler(CommandHandler("admins", self.list_admins))
        application.add_handler(CommandHandler("logs", self.view_logs))
        application.add_handler(CommandHandler("mystats", self.my_stats))

        # Templates
        application.add_handler(CommandHandler("templates", self.list_templates))
        application.add_handler(template_handler)

        # Broadcasts
        application.add_handler(broadcast_handler)
        application.add_handler(schedule_handler)
        application.add_handler(CommandHandler("scheduled", self.list_scheduled))
        application.add_handler(CommandHandler("cancel_scheduled", self.cancel_scheduled_command))

        # Signal suggestions
        application.add_handler(signal_handler)

        # Subscription
        application.add_handler(subscribe_handler)
        application.add_handler(
            MessageHandler(
                filters.Regex(r"^(Hello|Hi|Hey|Good morning|Good afternoon|Good evening|What's up|Howdy|Greetings|Hey there)$"),
                self.handle_greeting,
            )
        )

        # Error handler
        application.add_error_handler(self.error_handler)

        # Schedule checker (every minute)
        application.job_queue.run_repeating(
            self.process_scheduled_broadcasts,
            interval=60,
            first=10
        )

        return application

    async def handle_greeting(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle greetings"""
        await update.message.reply_text("Hello! How can I assist you today?")

    async def subscribe_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start the subscription conversation"""
        keyboard = [
            [InlineKeyboardButton("Deriv VIP", callback_data="vip_deriv")],
            [InlineKeyboardButton("Currencies VIP", callback_data="vip_currencies")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "Which VIP/Premium group do you wish to be added to?",
            reply_markup=reply_markup
        )
        return WAITING_VIP_GROUP

    async def receive_vip_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle VIP group selection"""
        query = update.callback_query
        await query.answer()
        context.user_data['vip_group'] = query.data

        if query.data == "vip_deriv":
            await query.edit_message_text(
                "Have you created an account following this procedure: https://t.me/forexbactest/1341?"
            )
            return WAITING_ACCOUNT_CREATION_CONFIRMATION
        elif query.data == "vip_currencies":
            keyboard = [
                [InlineKeyboardButton("OctaFX", callback_data="broker_octafx")],
                [InlineKeyboardButton("Vantage", callback_data="broker_vantage")],
                [InlineKeyboardButton("LiteFinance", callback_data="broker_litefinance")],
                [InlineKeyboardButton("JustMarkets", callback_data="broker_justmarkets")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                "Please select your affiliated broker:",
                reply_markup=reply_markup
            )
            return WAITING_BROKER_CHOICE

    async def receive_account_creation_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle account creation confirmation"""
        await update.message.reply_text("When did you create the account? (e.g., today, yesterday, YYYY-MM-DD)")
        return WAITING_ACCOUNT_DATE

    async def receive_account_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle account creation date"""
        # A more sophisticated date parsing could be added here
        if "today" in update.message.text.lower() or "yesterday" in update.message.text.lower():
            await update.message.reply_text("Please wait up to 24 hours for the account to reflect in the system.")
            return ConversationHandler.END
        else:
            await update.message.reply_text("Please provide your CR number in the format 'CR12345'.")
            return WAITING_CR_NUMBER

    async def receive_cr_number(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle CR number and check against the list"""
        cr_number = update.message.text.strip().upper()
        if cr_number in self.cr_numbers:
            await update.message.reply_text(
                "I can verify that you are tagged under us. Please proceed to fund your account with a minimum of $50 and send me a screenshot."
            )
            return WAITING_SCREENSHOT
        else:
            await update.message.reply_text(
                "Are you tagged under our partner, Kennedynespot? (yes/no)"
            )
            return WAITING_KENNEDYNESPOT_CONFIRMATION

    async def receive_screenshot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the screenshot and check the balance"""
        photo_file = await update.message.photo[-1].get_file()
        photo_bytes = await photo_file.download_as_bytearray()

        try:
            text = pytesseract.image_to_string(Image.open(io.BytesIO(photo_bytes)))
            # A simple regex to find numbers with a dollar sign
            matches = re.findall(r'\$?(\d+\.\d{2})', text)
            if matches:
                balance = float(matches[0])
                if balance >= 50:
                    user_id = update.effective_user.id
                    self.db.add_subscriber(user_id)
                    await update.message.reply_text(
                        "✅ Thank you! You have been added to the subscribers list."
                    )
                    return ConversationHandler.END
                else:
                    await update.message.reply_text(
                        "The balance in the screenshot is less than $50. Please fund your account and try again."
                    )
                    return WAITING_SCREENSHOT
            else:
                await update.message.reply_text(
                    "I could not detect a balance in the screenshot. Please try again with a clearer image."
                )
                return WAITING_SCREENSHOT
        except Exception as e:
            logger.error(f"Error processing screenshot: {e}")
            await update.message.reply_text("Sorry, I had trouble processing the image. Please try again.")
            return WAITING_SCREENSHOT

    async def receive_kennedynespot_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle Kennedynespot confirmation"""
        if "yes" in update.message.text.lower():
            await update.message.reply_text(
                "Please send us a direct message with a screenshot of the confirmation from our partner."
            )
        else:
            await update.message.reply_text(
                "Please follow the tagging guide: https://t.me/derivaccountopeningguide/66 and return after 24 hours to check again."
            )
        return ConversationHandler.END

    async def receive_broker_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle broker selection for Currencies VIP"""
        query = update.callback_query
        await query.answer()
        context.user_data['broker'] = query.data.split('_')[1]
        await query.edit_message_text("Please provide the full name on your account.")
        return WAITING_ACCOUNT_NAME

    async def receive_account_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive the user's full name"""
        context.user_data['account_name'] = update.message.text
        await update.message.reply_text("Please provide your account number.")
        return WAITING_ACCOUNT_NUMBER

    async def receive_account_number(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive the user's account number"""
        context.user_data['account_number'] = update.message.text
        await update.message.reply_text("Please provide your Telegram ID (e.g., @username or your user ID).")
        return WAITING_TELEGRAM_ID

    async def receive_telegram_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive Telegram ID and send for approval"""
        context.user_data['telegram_id'] = update.message.text
        user_info = (
            f"New Currencies VIP Request:\n\n"
            f"Broker: {context.user_data['broker']}\n"
            f"Account Name: {context.user_data['account_name']}\n"
            f"Account Number: {context.user_data['account_number']}\n"
            f"Telegram ID: {context.user_data['telegram_id']}\n"
            f"User ID: {update.effective_user.id}"
        )

        for admin_id in self.super_admin_ids:
            try:
                await context.bot.send_message(chat_id=admin_id, text=user_info)
            except Exception as e:
                logger.error(f"Failed to notify super admin {admin_id}: {e}")

        await update.message.reply_text(
            "Thank you! Your details have been sent to the admins for approval. You will be notified once it's reviewed."
        )
        return ConversationHandler.END
        
    async def receive_schedule_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive and parse schedule time"""
        try:
            # Simple parsing for '1h 30m' or '2d'
            time_str = update.message.text.lower()
            delta = timedelta()
            parts = time_str.replace('d', 'd ').replace('h', 'h ').replace('m', 'm ').split()
            
            val = 0
            for part in parts:
                if part.isdigit():
                    val = int(part)
                elif 'd' in part:
                    delta += timedelta(days=val)
                elif 'h' in part:
                    delta += timedelta(hours=val)
                elif 'm' in part:
                    delta += timedelta(minutes=val)
            
            if delta == timedelta():
                # Try parsing as absolute time
                scheduled_time = datetime.fromisoformat(time_str)
            else:
                scheduled_time = datetime.now() + delta

            context.user_data['scheduled_time'] = scheduled_time.timestamp()

            keyboard = [
                [InlineKeyboardButton("🔁 Once", callback_data="repeat_once")],
                [InlineKeyboardButton("🔁 Daily", callback_data="repeat_daily")],
                [InlineKeyboardButton("🔁 Weekly", callback_data="repeat_weekly")],
                [InlineKeyboardButton("🔁 Monthly", callback_data="repeat_monthly")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Set repeat interval:", reply_markup=reply_markup)
            return WAITING_SCHEDULE_REPEAT
        except ValueError:
            await update.message.reply_text("Invalid time format. Use 'YYYY-MM-DD HH:MM' or '1h 30m'.")
            return WAITING_SCHEDULE_TIME
    
    async def receive_schedule_repeat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive schedule repeat choice"""
        query = update.callback_query
        await query.answer()

        repeat = query.data.split('_')[1]
        context.user_data['repeat'] = repeat

        return await self.ask_target_audience(query, context, scheduled=True)

    async def save_template_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start save template conversation"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_TEMPLATES):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END

        await update.message.reply_text("Send me the message to save as a template.")
        context.user_data.clear()
        return WAITING_TEMPLATE_MESSAGE

    async def receive_template_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive template message"""
        context.user_data['template_message'] = update.message
        await update.message.reply_text("Enter a name for this template:")
        return WAITING_TEMPLATE_NAME

    async def receive_template_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive template name"""
        context.user_data['template_name'] = update.message.text
        await update.message.reply_text("Enter a category for this template (e.g., 'welcome', 'promo'):")
        return WAITING_TEMPLATE_CATEGORY

    async def receive_template_category(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive template category and save"""
        category = update.message.text
        name = context.user_data['template_name']
        message = context.user_data['template_message']
        
        message_data = {}
        if message.text:
            message_data['type'] = 'text'
            message_data['content'] = message.text
        elif message.photo:
            message_data['type'] = 'photo'
            message_data['file_id'] = message.photo[-1].file_id
            message_data['caption'] = message.caption
        elif message.video:
            message_data['type'] = 'video'
            message_data['file_id'] = message.video.file_id
            message_data['caption'] = message.caption
        elif message.document:
            message_data['type'] = 'document'
            message_data['file_id'] = message.document.file_id
            message_data['caption'] = message.caption
            
        template_id = self.db.save_template(name, message_data, category, update.effective_user.id)
        if template_id:
            await update.message.reply_text(f"✅ Template '{name}' saved successfully!")
        else:
            await update.message.reply_text("❌ Failed to save template.")
        
        return ConversationHandler.END
        
    async def add_admin_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start add admin conversation"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_ADMINS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END

        await update.message.reply_text("Send me the user ID of the new admin.")
        return WAITING_ADMIN_ID

    async def receive_admin_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive admin ID and ask for role"""
        try:
            user_id = int(update.message.text)
            context.user_data['new_admin_id'] = user_id

            keyboard = [
                [InlineKeyboardButton("Broadcaster", callback_data="role_broadcaster")],
                [InlineKeyboardButton("Moderator", callback_data="role_moderator")],
                [InlineKeyboardButton("Admin", callback_data="role_admin")],
                [InlineKeyboardButton("Super Admin", callback_data="role_super_admin")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Choose a role for the new admin:", reply_markup=reply_markup)
            return WAITING_ADMIN_ROLE
        except ValueError:
            await update.message.reply_text("Invalid user ID. Please send a numeric ID.")
            return WAITING_ADMIN_ID

    async def receive_admin_role(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive admin role and add admin"""
        query = update.callback_query
        await query.answer()

        role_str = query.data.split('_')[1]
        role = AdminRole(f"{role_str}")
        user_id = context.user_data['new_admin_id']

        if self.db.add_admin(user_id, role, query.from_user.id):
            await query.edit_message_text(f"✅ User {user_id} is now an admin with role '{role.value}'.")
        else:
            await query.edit_message_text(f"❌ Failed to add admin.")

        return ConversationHandler.END

    async def remove_admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /removeadmin command"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_ADMINS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        if not context.args:
            await update.message.reply_text("❌ Please provide a user ID: /removeadmin <user_id>")
            return

        try:
            user_id = int(context.args[0])
            if self.db.remove_admin(user_id, update.effective_user.id):
                await update.message.reply_text(f"✅ Admin {user_id} has been removed.")
            else:
                await update.message.reply_text(f"❌ Admin {user_id} not found.")
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID.")
    
    async def list_admins(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /admins command"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_ADMINS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        admins = self.db.get_all_admins()
        if not admins:
            await update.message.reply_text("No admins found.")
            return

        admin_list = "\n".join([f"• {a['user_id']} ({a['role']})" for a in admins])
        await update.message.reply_text(f"👨‍💼 Admins:\n{admin_list}")

    async def view_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /logs command"""
        if not self.has_permission(update.effective_user.id, Permission.VIEW_LOGS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return
            
        logs = self.db.get_activity_logs()
        if not logs:
            await update.message.reply_text("No activity logs found.")
            return

        log_list = "\n".join([
            f"• {datetime.fromtimestamp(log['timestamp']).strftime('%Y-%m-%d %H:%M')} "
            f"| {log['user_id']} | {log['action']} | {log.get('details', {})}"
            for log in logs
        ])
        await update.message.reply_text(f"📜 Activity Logs:\n{log_list}")

    async def my_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /mystats command"""
        user_id = update.effective_user.id
        if not self.is_admin(user_id):
            await update.message.reply_text("❌ This command is for admins only.")
            return

        stats = self.db.get_admin_stats(user_id)
        stats_text = (
            f"📊 Your Statistics\n\n"
            f"📢 Broadcasts Sent: {stats.get('broadcasts', 0)}\n"
            f"📝 Templates Created: {stats.get('templates', 0)}\n"
            f"⏰ Broadcasts Scheduled: {stats.get('scheduled', 0)}"
        )
        await update.message.reply_text(stats_text)

    async def list_templates(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /templates command"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_TEMPLATES):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        templates = self.db.get_all_templates()
        if not templates:
            await update.message.reply_text("No templates found.")
            return

        template_list = "\n".join([f"• {t['name']} ({t['category']})" for t in templates])
        await update.message.reply_text(f"📝 Templates:\n{template_list}")

    async def list_scheduled(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /scheduled command"""
        if not self.has_permission(update.effective_user.id, Permission.SCHEDULE_BROADCASTS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        broadcasts = self.db.get_scheduled_broadcasts()
        if not broadcasts:
            await update.message.reply_text("No scheduled broadcasts.")
            return

        broadcast_list = "\n".join([
            f"• ID: {str(b['_id'])[-6:]} | "
            f"{datetime.fromtimestamp(b['scheduled_time']).strftime('%Y-%m-%d %H:%M')}"
            for b in broadcasts
        ])
        await update.message.reply_text(f"⏰ Scheduled Broadcasts:\n{broadcast_list}")

    async def cancel_scheduled_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cancel_scheduled command"""
        if not self.has_permission(update.effective_user.id, Permission.SCHEDULE_BROADCASTS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        if not context.args:
            await update.message.reply_text("❌ Please provide a broadcast ID: /cancel_scheduled <id>")
            return

        broadcast_id = context.args[0]
        if self.db.cancel_scheduled_broadcast(broadcast_id, update.effective_user.id):
            await update.message.reply_text(f"✅ Scheduled broadcast {broadcast_id} cancelled.")
        else:
            await update.message.reply_text(f"❌ Broadcast {broadcast_id} not found or already processed.")


def main():
    """Main function"""
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    ADMIN_IDS = os.getenv('ADMIN_IDS', '').split(',')
    MONGODB_URI = os.getenv('MONGODB_URI')

    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN required")

    if not ADMIN_IDS or ADMIN_IDS == ['']:
        raise ValueError("ADMIN_IDS required")

    if not MONGODB_URI:
        raise ValueError("MONGODB_URI required")

    try:
        admin_ids = [int(admin_id.strip()) for admin_id in ADMIN_IDS if admin_id.strip()]
    except ValueError:
        raise ValueError("ADMIN_IDS must be comma-separated integers")

    logger.info("Connecting to MongoDB...")
    mongo_handler = MongoDBHandler(MONGODB_URI)

    bot = BroadcastBot(BOT_TOKEN, admin_ids, mongo_handler)
    application = bot.create_application()

    port = int(os.getenv('PORT', 8000))

    logger.info(f"Starting bot with {len(admin_ids)} super admin(s)")
    logger.info(f"Health server on port {port}")

    bot.run_health_server(port)

    time.sleep(2)

    logger.info("Starting Telegram bot...")
    try:
        application.run_polling()
    finally:
        mongo_handler.close()


if __name__ == '__main__':
    main()
