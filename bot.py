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

            # Create indexes
            self.users_collection.create_index('user_id', unique=True)
            self.subscribers_collection.create_index('user_id', unique=True)
            self.admins_collection.create_index('user_id', unique=True)
            self.templates_collection.create_index('created_by')
            self.scheduled_broadcasts_collection.create_index('scheduled_time')
            self.activity_logs_collection.create_index([('timestamp', -1)])

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

    def get_stats(self) -> Dict:
        """Get bot statistics"""
        try:
            total_users = self.users_collection.count_documents({})
            total_subscribers = self.subscribers_collection.count_documents({})
            total_admins = self.admins_collection.count_documents({})
            total_templates = self.templates_collection.count_documents({})
            total_scheduled = self.scheduled_broadcasts_collection.count_documents({'status': 'pending'})

            return {
                'total_users': total_users,
                'subscribers': total_subscribers,
                'non_subscribers': total_users - total_subscribers,
                'admins': total_admins,
                'templates': total_templates,
                'scheduled_broadcasts': total_scheduled
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
            # Open image
            image = Image.open(io.BytesIO(image_bytes))

            # Convert to RGB if necessary
            if image.mode != 'RGB':
                image = image.convert('RGB')

            # Create drawing context
            draw = ImageDraw.Draw(image)

            # Calculate watermark size and position
            width, height = image.size
            font_size = int(min(width, height) * 0.05)  # 5% of smallest dimension

            # Try to use a nice font, fall back to default
            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
            except:
                font = ImageFont.load_default()

            # Get text bounding box
            bbox = draw.textbbox((0, 0), watermark_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            # Position watermark in bottom-right corner with padding
            padding = int(min(width, height) * 0.02)
            x = width - text_width - padding
            y = height - text_height - padding

            # Draw semi-transparent background
            background_padding = int(font_size * 0.3)
            draw.rectangle(
                [x - background_padding, y - background_padding,
                 x + text_width + background_padding, y + text_height + background_padding],
                fill=(0, 0, 0, 180)
            )

            # Draw watermark text
            draw.text((x, y), watermark_text, fill=(255, 255, 255, 230), font=font)

            # Save to bytes
            output = io.BytesIO()
            image.save(output, format='JPEG', quality=95)
            output.seek(0)

            return output.getvalue()
        except Exception as e:
            logger.error(f"Error adding watermark: {e}")
            return image_bytes  # Return original if watermarking fails


class BroadcastBot:
    def __init__(self, token: str, super_admin_ids: List[int], mongo_handler: MongoDBHandler):
        self.token = token
        self.super_admin_ids = super_admin_ids
        self.db = mongo_handler
        self.watermarker = ImageWatermarker()

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

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user = update.effective_user
        user_id = user.id

        # Save user to database
        self.db.add_user(user_id, user.username, user.first_name)

        if self.is_admin(user_id):
            role = self.get_admin_role(user_id)
            message = (
                f"🔧 Admin Panel ({role.value.replace('_', ' ').title()})\n\n"
                "📢 Broadcasting:\n"
                "/broadcast - Start broadcasting\n"
                "/schedule - Schedule a broadcast\n"
                "/scheduled - View scheduled broadcasts\n\n"
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
                "Commands:\n"
                "/subscribe - Subscribe to broadcasts\n"
                "/unsubscribe - Unsubscribe from broadcasts"
            )
            await update.message.reply_text(message)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        await self.start(update, context)

    async def subscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribe command"""
        user = update.effective_user
        user_id = user.id

        self.db.add_user(user_id, user.username, user.first_name)

        if self.db.is_subscriber(user_id):
            await update.message.reply_text("✅ You're already subscribed to broadcasts!")
        else:
            self.db.add_subscriber(user_id)
            await update.message.reply_text("🔔 Successfully subscribed to broadcasts!")

    async def unsubscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /unsubscribe command"""
        user_id = update.effective_user.id

        if self.db.remove_subscriber(user_id):
            await update.message.reply_text("🔕 Successfully unsubscribed from broadcasts!")
        else:
            await update.message.reply_text("❌ You're not currently subscribed.")

    async def add_subscriber_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /add command"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_USERS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
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
            f"⏰ Scheduled: {stats.get('scheduled_broadcasts', 0)}"
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

    # Admin Management Commands
    async def add_admin_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start add admin conversation"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_ADMINS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END

        await update.message.reply_text(
            "👨‍💼 Add New Admin\n\n"
            "Please send me the user ID of the person you want to make an admin.\n\n"
            "Send /cancel to cancel."
        )
        return WAITING_ADMIN_ID

    async def receive_admin_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive admin ID"""
        try:
            user_id = int(update.message.text.strip())
            context.user_data['new_admin_id'] = user_id

            keyboard = [
                [InlineKeyboardButton("🔴 Super Admin (All permissions)", callback_data="role_super_admin")],
                [InlineKeyboardButton("🟠 Admin (Most permissions)", callback_data="role_admin")],
                [InlineKeyboardButton("🟡 Moderator (Approval & viewing)", callback_data="role_moderator")],
                [InlineKeyboardButton("🟢 Broadcaster (Broadcasting only)", callback_data="role_broadcaster")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"👤 User ID: {user_id}\n\n"
                "Please select the admin role:",
                reply_markup=reply_markup
            )
            return WAITING_ADMIN_ROLE
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. Please send a numeric ID.")
            return WAITING_ADMIN_ID

    async def receive_admin_role(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive admin role selection"""
        query = update.callback_query
        await query.answer()

        role_mapping = {
            "role_super_admin": AdminRole.SUPER_ADMIN,
            "role_admin": AdminRole.ADMIN,
            "role_moderator": AdminRole.MODERATOR,
            "role_broadcaster": AdminRole.BROADCASTER
        }

        role = role_mapping.get(query.data)
        user_id = context.user_data['new_admin_id']

        if self.db.add_admin(user_id, role, query.from_user.id):
            await query.edit_message_text(
                f"✅ Successfully added user {user_id} as {role.value.replace('_', ' ').title()}!\n\n"
                "They now have access to admin commands."
            )
        else:
            await query.edit_message_text("❌ Failed to add admin. Please try again.")

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

            if user_id in self.super_admin_ids:
                await update.message.reply_text("❌ Cannot remove super admin.")
                return

            if self.db.remove_admin(user_id, update.effective_user.id):
                await update.message.reply_text(f"✅ Successfully removed admin {user_id}!")
            else:
                await update.message.reply_text("❌ User is not an admin.")
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. Please provide a numeric ID.")

    async def list_admins(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /admins command"""
        if not self.has_permission(update.effective_user.id, Permission.VIEW_STATS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        admins = self.db.get_all_admins()

        if not admins:
            await update.message.reply_text("👨‍💼 No admins found.")
            return

        message = "👨‍💼 Admin List:\n\n"
        for admin in admins:
            role_emoji = {
                'super_admin': '🔴',
                'admin': '🟠',
                'moderator': '🟡',
                'broadcaster': '🟢'
            }
            emoji = role_emoji.get(admin['role'], '⚪')
            role_name = admin['role'].replace('_', ' ').title()
            message += f"{emoji} {admin['user_id']} - {role_name}\n"

        await update.message.reply_text(message)

    async def view_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /logs command"""
        if not self.has_permission(update.effective_user.id, Permission.VIEW_LOGS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        logs = self.db.get_activity_logs(limit=20)

        if not logs:
            await update.message.reply_text("📋 No activity logs found.")
            return

        message = "📋 Recent Activity (Last 20):\n\n"
        for log in logs:
            timestamp = datetime.fromtimestamp(log['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            action = log['action'].replace('_', ' ').title()
            message += f"🕐 {timestamp}\n"
            message += f"👤 User: {log['user_id']}\n"
            message += f"⚡ Action: {action}\n"
            if log.get('details'):
                message += f"📝 Details: {log['details']}\n"
            message += "\n"

        if len(message) > 4000:
            chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
            for chunk in chunks:
                await update.message.reply_text(chunk)
        else:
            await update.message.reply_text(message)

    async def my_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /mystats command"""
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ This command is for admins only.")
            return

        user_id = update.effective_user.id
        stats = self.db.get_admin_stats(user_id)
        role = self.get_admin_role(user_id)

        message = (
            f"📊 Your Admin Statistics\n\n"
            f"👤 Role: {role.value.replace('_', ' ').title()}\n"
            f"📢 Broadcasts Sent: {stats.get('broadcasts', 0)}\n"
            f"📝 Templates Created: {stats.get('templates', 0)}\n"
            f"⏰ Scheduled Broadcasts: {stats.get('scheduled', 0)}"
        )
        await update.message.reply_text(message)

    # Template Management
    async def list_templates(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /templates command"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_TEMPLATES):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        templates = self.db.get_all_templates()

        if not templates:
            await update.message.reply_text(
                "📝 No templates found.\n\n"
                "Use /savetemplate during a broadcast to save a template."
            )
            return

        # Group by category
        categories = {}
        for template in templates:
            category = template.get('category', 'Uncategorized')
            if category not in categories:
                categories[category] = []
            categories[category].append(template)

        message = "📝 Message Templates:\n\n"
        for category, temps in categories.items():
            message += f"📁 {category}:\n"
            for temp in temps:
                temp_id = str(temp['_id'])[-6:]  # Last 6 chars of ID
                usage = temp.get('usage_count', 0)
                message += f"  • {temp['name']} (ID: {temp_id}, Used: {usage}x)\n"
            message += "\n"

        message += "Use /usetemplate <name> to use a template\n"
        message += "Use /deletetemplate <name> to delete a template"

        await update.message.reply_text(message)

    async def save_template_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start save template conversation"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_TEMPLATES):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END

        await update.message.reply_text(
            "📝 Save as Template\n\n"
            "Please send me the message you want to save as a template.\n"
            "You can send text, photos, videos, or documents.\n\n"
            "Send /cancel to cancel."
        )
        return WAITING_TEMPLATE_MESSAGE

    async def receive_template_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive template message"""
        context.user_data['template_message'] = update.message

        await update.message.reply_text(
            "📝 Template Name\n\n"
            "Please provide a name for this template:"
        )
        return WAITING_TEMPLATE_NAME

    async def receive_template_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive template name"""
        context.user_data['template_name'] = update.message.text.strip()

        await update.message.reply_text(
            "📁 Template Category\n\n"
            "Please provide a category for this template\n"
            "(e.g., 'Signals', 'Updates', 'Announcements', 'Educational'):"
        )
        return WAITING_TEMPLATE_CATEGORY

    async def receive_template_category(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive template category and save"""
        category = update.message.text.strip()
        name = context.user_data['template_name']
        message = context.user_data['template_message']

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

        template_id = self.db.save_template(name, message_data, category, update.effective_user.id)

        if template_id:
            await update.message.reply_text(
                f"✅ Template saved successfully!\n\n"
                f"📝 Name: {name}\n"
                f"📁 Category: {category}\n\n"
                f"Use /templates to view all templates."
            )
        else:
            await update.message.reply_text("❌ Failed to save template. Please try again.")

        return ConversationHandler.END

    # Scheduled Broadcasts
    async def schedule_broadcast_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start schedule broadcast conversation"""
        if not self.has_permission(update.effective_user.id, Permission.SCHEDULE_BROADCASTS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END

        await update.message.reply_text(
            "⏰ Schedule Broadcast\n\n"
            "Please send me the message you want to schedule.\n"
            "You can send text, photos, videos, or documents.\n\n"
            "Send /cancel to cancel."
        )
        return WAITING_MESSAGE

    async def schedule_set_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Set schedule time"""
        context.user_data['schedule_message'] = context.user_data.get('broadcast_message')

        await update.message.reply_text(
            "⏰ Set Schedule Time\n\n"
            "Send the time when you want this broadcast to be sent.\n\n"
            "Formats supported:\n"
            "• Minutes from now: '30m' or '30'\n"
            "• Hours from now: '2h'\n"
            "• Specific time today: '14:30' or '2:30 PM'\n"
            "• Specific date and time: '2024-12-25 10:00'\n\n"
            "Examples:\n"
            "• '30' = 30 minutes from now\n"
            "• '2h' = 2 hours from now\n"
            "• '14:30' = Today at 2:30 PM"
        )
        return WAITING_SCHEDULE_TIME

    async def receive_schedule_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive schedule time"""
        time_input = update.message.text.strip()

        try:
            scheduled_time = self.parse_time_input(time_input)
            context.user_data['scheduled_time'] = scheduled_time

            keyboard = [
                [InlineKeyboardButton("📅 Once (No repeat)", callback_data="repeat_once")],
                [InlineKeyboardButton("🔄 Daily", callback_data="repeat_daily")],
                [InlineKeyboardButton("📆 Weekly", callback_data="repeat_weekly")],
                [InlineKeyboardButton("📅 Monthly", callback_data="repeat_monthly")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            scheduled_dt = datetime.fromtimestamp(scheduled_time)
            await update.message.reply_text(
                f"⏰ Scheduled for: {scheduled_dt.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                "Do you want this broadcast to repeat?",
                reply_markup=reply_markup
            )
            return WAITING_SCHEDULE_REPEAT
        except ValueError as e:
            await update.message.reply_text(f"❌ Invalid time format: {e}\nPlease try again.")
            return WAITING_SCHEDULE_TIME

    def parse_time_input(self, time_input: str) -> float:
        """Parse time input string to timestamp"""
        now = datetime.now()

        # Minutes from now (just number or with 'm')
        if time_input.isdigit() or time_input.endswith('m'):
            minutes = int(time_input.replace('m', ''))
            return (now + timedelta(minutes=minutes)).timestamp()

        # Hours from now
        if time_input.endswith('h'):
            hours = int(time_input.replace('h', ''))
            return (now + timedelta(hours=hours)).timestamp()

        # Time today (HH:MM or HH:MM AM/PM)
        try:
            if ':' in time_input and len(time_input.split()) <= 2:
                if 'AM' in time_input.upper() or 'PM' in time_input.upper():
                    time_obj = datetime.strptime(time_input, '%I:%M %p')
                else:
                    time_obj = datetime.strptime(time_input, '%H:%M')

                scheduled = now.replace(hour=time_obj.hour, minute=time_obj.minute, second=0, microsecond=0)
                if scheduled < now:
                    scheduled += timedelta(days=1)
                return scheduled.timestamp()
        except:
            pass

        # Full date and time
        try:
            dt = datetime.strptime(time_input, '%Y-%m-%d %H:%M')
            if dt < now:
                raise ValueError("Cannot schedule in the past")
            return dt.timestamp()
        except:
            pass

        raise ValueError("Invalid time format")

    async def receive_schedule_repeat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive schedule repeat option"""
        query = update.callback_query
        await query.answer()

        repeat_options = {
            'repeat_once': 'once',
            'repeat_daily': 'daily',
            'repeat_weekly': 'weekly',
            'repeat_monthly': 'monthly'
        }

        repeat = repeat_options.get(query.data, 'once')
        context.user_data['schedule_repeat'] = repeat

        # Now ask for target audience
        return await self.ask_target_audience(query, context, scheduled=True)

    async def finalize_scheduled_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Finalize and save scheduled broadcast"""
        query = update.callback_query
        await query.answer()

        # Get target
        target_map = {
            'target_all': 'all',
            'target_subscribers': 'subscribers',
            'target_nonsubscribers': 'nonsubscribers'
        }
        target = target_map.get(query.data, 'all')

        # Get all data
        message = context.user_data.get('schedule_message') or context.user_data.get('broadcast_message')
        scheduled_time = context.user_data['scheduled_time']
        repeat = context.user_data['schedule_repeat']

        # Prepare message data
        message_data = {
            'type': 'text',
            'content': None,
            'inline_buttons': context.user_data.get('inline_buttons'),
            'protect_content': context.user_data.get('protect_content', False)
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

        # Save to database
        broadcast_id = self.db.schedule_broadcast(
            message_data,
            scheduled_time,
            repeat,
            query.from_user.id,
            target
        )

        if broadcast_id:
            scheduled_dt = datetime.fromtimestamp(scheduled_time)
            repeat_text = "✅ Will repeat: " + repeat.title() if repeat != 'once' else "📅 One-time broadcast"

            await query.edit_message_text(
                f"✅ Broadcast Scheduled!\n\n"
                f"⏰ Time: {scheduled_dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🎯 Target: {target.title()}\n"
                f"{repeat_text}\n"
                f"🆔 Broadcast ID: {str(broadcast_id)[-8:]}\n\n"
                f"Use /scheduled to view all scheduled broadcasts."
            )
        else:
            await query.edit_message_text("❌ Failed to schedule broadcast. Please try again.")

        return ConversationHandler.END

    async def list_scheduled(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /scheduled command"""
        if not self.has_permission(update.effective_user.id, Permission.SCHEDULE_BROADCASTS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        broadcasts = self.db.get_scheduled_broadcasts()

        if not broadcasts:
            await update.message.reply_text("📅 No scheduled broadcasts found.")
            return

        message = "📅 Scheduled Broadcasts:\n\n"
        for broadcast in broadcasts:
            broadcast_id = str(broadcast['_id'])[-8:]
            scheduled_dt = datetime.fromtimestamp(broadcast['scheduled_time'])
            target = broadcast['target'].title()
            repeat = broadcast['repeat'].title()

            message += f"🆔 ID: {broadcast_id}\n"
            message += f"⏰ Time: {scheduled_dt.strftime('%Y-%m-%d %H:%M')}\n"
            message += f"🎯 Target: {target}\n"
            message += f"🔄 Repeat: {repeat}\n"
            message += f"---\n"

        message += "\nUse /cancel_scheduled <ID> to cancel a broadcast"
        await update.message.reply_text(message)

    async def cancel_scheduled_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cancel_scheduled command"""
        if not self.has_permission(update.effective_user.id, Permission.SCHEDULE_BROADCASTS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        if not context.args:
            await update.message.reply_text("❌ Please provide broadcast ID: /cancel_scheduled <ID>")
            return

        broadcast_id = context.args[0]

        # Find the full ID
        broadcasts = self.db.get_scheduled_broadcasts()
        full_id = None
        for broadcast in broadcasts:
            if str(broadcast['_id']).endswith(broadcast_id):
                full_id = str(broadcast['_id'])
                break

        if not full_id:
            await update.message.reply_text("❌ Broadcast not found.")
            return

        if self.db.cancel_scheduled_broadcast(full_id, update.effective_user.id):
            await update.message.reply_text(f"✅ Scheduled broadcast {broadcast_id} cancelled!")
        else:
            await update.message.reply_text("❌ Failed to cancel broadcast.")

    async def start_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start broadcast conversation"""
        if not self.has_permission(update.effective_user.id, Permission.BROADCAST):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END

        message = (
            "📝 Start Broadcasting\n\n"
            "Please send me the message you want to broadcast.\n"
            "You can send text, photos, videos, or documents.\n\n"
            "💡 Tip: Use /templates to use a saved template\n\n"
            "Send /cancel to cancel this operation."
        )
        await update.message.reply_text(message)
        return WAITING_MESSAGE

    async def receive_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive the broadcast message"""
        context.user_data['broadcast_message'] = update.message
        
        # Check if it's an image and offer watermarking
        if update.message.photo:
            keyboard = [
                [InlineKeyboardButton("💧 Add Watermark", callback_data="watermark_yes")],
                [InlineKeyboardButton("❌ No Watermark", callback_data="watermark_no")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "💧 Watermark Image?\n\n"
                "Do you want to add a watermark to this image?",
                reply_markup=reply_markup
            )
            return WAITING_BUTTONS
        else:
            keyboard = [
                [InlineKeyboardButton("✅ Yes, add buttons", callback_data="add_buttons")],
                [InlineKeyboardButton("❌ No, skip buttons", callback_data="skip_buttons")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                "🔘 Do you want to add inline buttons to this message?",
                reply_markup=reply_markup
            )
            return WAITING_BUTTONS


    async def handle_watermark_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle watermark choice"""
        query = update.callback_query
        await query.answer()

        if query.data == "watermark_yes":
            # Download and watermark the image
            message = context.user_data['broadcast_message']
            photo = message.photo[-1]

            try:
                file = await context.bot.get_file(photo.file_id)
                image_bytes = await file.download_as_bytearray()

                # Add watermark
                watermarked_bytes = self.watermarker.add_watermark(bytes(image_bytes))

                # Save watermarked version
                context.user_data['watermarked_image'] = watermarked_bytes
                context.user_data['use_watermark'] = True

                await query.edit_message_text("✅ Watermark will be added to the image!")
            except Exception as e:
                logger.error(f"Error watermarking image: {e}")
                await query.edit_message_text("❌ Failed to add watermark. Continuing without watermark.")
                context.user_data['use_watermark'] = False
        else:
            context.user_data['use_watermark'] = False
            await query.edit_message_text("✅ No watermark will be added.")

        # Continue to buttons
        keyboard = [
            [InlineKeyboardButton("✅ Yes, add buttons", callback_data="add_buttons")],
            [InlineKeyboardButton("❌ No, skip buttons", callback_data="skip_buttons")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await context.bot.send_message(
            chat_id=query.from_user.id,
            text="🔘 Do you want to add inline buttons to this message?",
            reply_markup=reply_markup
        )
        return WAITING_BUTTONS
        
    async def handle_buttons_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle inline buttons choice"""
        query = update.callback_query
        await query.answer()

        if query.data == "add_buttons":
            message = (
                "🔘 Adding Inline Buttons\n\n"
                "Send button configurations in this format:\n"
                "Button Text 1|URL1\n"
                "Button Text 2|URL2\n\n"
                "Example:\n"
                "Visit Website|https://example.com\n"
                "Join Channel|https://t.me/channel\n\n"
                "Send /skip to continue without buttons."
            )
            await query.edit_message_text(message)
            return WAITING_BUTTONS
        else:
            context.user_data['inline_buttons'] = None
            return await self.ask_message_protection(update, context)

    async def receive_buttons(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive button configuration"""
        if update.message.text == "/skip":
            context.user_data['inline_buttons'] = None
        else:
            try:
                buttons = []
                lines = update.message.text.strip().split('\n')
                for line in lines:
                    if '|' in line:
                        text, url = line.split('|', 1)
                        buttons.append([InlineKeyboardButton(text.strip(), url=url.strip())])

                if buttons:
                    context.user_data['inline_buttons'] = InlineKeyboardMarkup(buttons)
                    await update.message.reply_text(f"✅ Added {len(buttons)} button(s) to the message!")
                else:
                    context.user_data['inline_buttons'] = None
                    await update.message.reply_text("❌ No valid buttons found. Continuing without buttons.")
            except Exception as e:
                await update.message.reply_text(f"❌ Error parsing buttons: {e}\nContinuing without buttons.")
                context.user_data['inline_buttons'] = None

        return await self.ask_message_protection(update, context)

    async def ask_message_protection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask about message protection settings"""
        keyboard = [
            [InlineKeyboardButton("🔒 Protect Message", callback_data="protect_yes")],
            [InlineKeyboardButton("🔓 Allow Forwarding", callback_data="protect_no")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        message = (
            "🛡️ Message Protection Settings\n\n"
            "🔒 **Protect**: Prevents forwarding/copying\n"
            "🔓 **Allow**: Normal message behavior"
        )

        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')

        return WAITING_PROTECTION

    async def handle_protection_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle message protection choice"""
        query = update.callback_query
        await query.answer()

        context.user_data['protect_content'] = (query.data == "protect_yes")
        protection_status = "🔒 Protected" if context.user_data['protect_content'] else "🔓 Unprotected"
        await query.edit_message_text(f"✅ {protection_status}")

        # Check if this is a scheduled broadcast
        if 'scheduled_time' in context.user_data:
            return await self.ask_target_audience(query, context, scheduled=True)
        else:
            return await self.ask_target_audience(update, context)

    async def ask_target_audience(self, update, context: ContextTypes.DEFAULT_TYPE, scheduled=False):
        """Ask who to send the broadcast to"""
        stats = self.db.get_stats()

        keyboard = [
            [InlineKeyboardButton("👥 All Users", callback_data="target_all")],
            [InlineKeyboardButton("🔔 Subscribers Only", callback_data="target_subscribers")],
            [InlineKeyboardButton("🔕 Non-subscribers Only", callback_data="target_nonsubscribers")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        stats_text = (
            f"👥 All Users: {stats['total_users']}\n"
            f"🔔 Subscribers: {stats['subscribers']}\n"
            f"🔕 Non-subscribers: {stats['non_subscribers']}"
        )

        message = f"🎯 Choose Target Audience\n\n{stats_text}\n\nWho should receive this broadcast?"

        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.message.reply_text(message, reply_markup=reply_markup)
        elif hasattr(update, 'message'):
            await update.message.reply_text(message, reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=update.from_user.id, text=message, reply_markup=reply_markup)

        if scheduled:
            return WAITING_TARGET  # Will handle scheduled broadcast
        else:
            return WAITING_TARGET  # Will handle immediate broadcast

    async def handle_target_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle target audience choice and send broadcast"""
        query = update.callback_query
        await query.answer()

        # Check if this is a scheduled broadcast
        if 'scheduled_time' in context.user_data:
            return await self.finalize_scheduled_broadcast(update, context)

        # Immediate broadcast
        all_users = self.db.get_all_users()
        subscribers = self.db.get_all_subscribers()

        if query.data == "target_all":
            target_users = all_users
            audience_name = "All Users"
        elif query.data == "target_subscribers":
            target_users = subscribers
            audience_name = "Subscribers"
        else:
            target_users = all_users - subscribers
            audience_name = "Non-subscribers"

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
                        # Send watermarked image
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
                await asyncio.sleep(0.05)  # Rate limiting
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

                    if target == 'all':
                        target_users = all_users
                    elif target == 'subscribers':
                        target_users = subscribers
                    else:
                        target_users = all_users - subscribers

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

        # Basic commands
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help_command))
        application.add_handler(CommandHandler("subscribe", self.subscribe))
        application.add_handler(CommandHandler("unsubscribe", self.unsubscribe))
        application.add_handler(CommandHandler("add", self.add_subscriber_command))
        application.add_handler(CommandHandler("stats", self.stats))
        application.add_handler(CommandHandler("subscribers", self.list_subscribers))

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

        # Error handler
        application.add_error_handler(self.error_handler)

        # Schedule checker (every minute)
        application.job_queue.run_repeating(
            self.process_scheduled_broadcasts,
            interval=60,
            first=10
        )

        return application


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

    import time
    time.sleep(2)

    logger.info("Starting Telegram bot...")
    try:
        application.run_polling()
    finally:
        mongo_handler.close()


if __name__ == '__main__':
    main()
