import os
import logging
import asyncio
from typing import Dict, List, Optional
from aiohttp import web
import threading
from datetime import datetime, timedelta, time as dt_time, timezone # <-- FIX 1: Renamed import
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    ContextTypes,
    ApplicationHandlerStop
)
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from PIL import Image, ImageDraw, ImageFont
import io
import time
from enum import Enum
import re
import pytesseract
import finnhub
from telegram.constants import ParseMode
import random
from typing import List, Dict, Optional
import tweepy

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Finnhub API Configuration ---
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
if not FINNHUB_API_KEY:
    logger.warning("FINNHUB_API_KEY is not set. /news and /calendar commands will be disabled.")
# ---------------------------------

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
    WAITING_DECLINE_REASON,
    WAITING_SIGNAL_RATING,
    WAITING_SIGNAL_REJECTION_REASON,# New state for rating signals
) = range(12, 25)


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

class PerformanceTransparency:
    """Show real, auditable performance"""
    
    @staticmethod
    async def show_verified_performance(update: Update, context: ContextTypes.DEFAULT_TYPE, db):
        """Display verified signal performance - builds trust"""
        
        # Calculate last 30 days performance
        thirty_days_ago = time.time() - (30 * 86400)
        
        pipeline = [
            {
                '$match': {
                    'status': 'approved',
                    'reviewed_at': {'$gte': thirty_days_ago},
                    'rating': {'$exists': True}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_signals': {'$sum': 1},
                    'avg_rating': {'$avg': '$rating'},
                    'excellent_signals': {
                        '$sum': {'$cond': [{'$gte': ['$rating', 4]}, 1, 0]}
                    }
                }
            }
        ]
        
        stats = list(db.signal_suggestions_collection.aggregate(pipeline))
        
        if not stats:
            await update.message.reply_text("📊 No performance data available yet for the last 30 days.")
            return
        
        data = stats[0]
        total = data['total_signals']
        avg_rating = data['avg_rating']
        excellent = data['excellent_signals']
        win_rate = (excellent / total * 100) if total > 0 else 0
        
        message = (
            "📊 <b>PipSage Performance (Last 30 Days)</b>\n\n"
            
            f"✅ Signals Shared: {total}\n"
            f"⭐ Average Rating: {avg_rating:.1f}/5.0\n"
            f"🎯 Quality Rate: {win_rate:.1f}% (4+ stars)\n\n"
            
            "📈 <b>Verified & Transparent</b>\n"
            "Every signal is rated by our admin team after results.\n"
            "No fake claims, no hidden losses.\n\n"
            
            "Try us: /subscribe"
        )
        
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

class AchievementSystem:
    """Award badges for milestones"""
    
    ACHIEVEMENTS = {
        'first_signal': {
            'name': '🌱 First Signal',
            'description': 'Submitted your first signal',
            'reward': 'Unlocked signal suggestions'
        },
        'approved_signal': {
            'name': '✅ Approved',
            'description': 'Had a signal approved by admins',
            'reward': '+1 to daily limit'
        },
        'five_star': {
            'name': '⭐⭐⭐⭐⭐ Perfect',
            'description': 'Received a 5-star rating',
            'reward': 'Featured in leaderboard'
        },
        'consistent': {
            'name': '🔥 Consistent',
            'description': '7 days of activity',
            'reward': '+1 to daily limit'
        },
        'top_10': {
            'name': '🏆 Top 10',
            'description': 'Reached top 10 on leaderboard',
            'reward': 'Special badge on signals'
        },
        'elite': {
            'name': '💎 Elite Trader',
            'description': '4.5+ avg rating, 20+ signals',
            'reward': 'Unlimited daily signals'
        }
    }
    
    @staticmethod
    async def check_and_award_achievements(user_id: int, context: ContextTypes.DEFAULT_TYPE, db):
        """Check if user earned new achievements"""
        
        user = db.users_collection.find_one({'user_id': user_id})
        if not user:
            return [] # User not found
            
        current_achievements = set(user.get('achievements', []))
        
        # Check conditions
        signal_stats = db.get_user_signal_stats(user_id)
        avg_rating = db.get_user_average_rating(user_id)
        
        new_achievements = []
        
        # First signal
        if 'first_signal' not in current_achievements and signal_stats['total'] >= 1:
            new_achievements.append('first_signal')
        
        # First approval
        if 'approved_signal' not in current_achievements and signal_stats['approved'] >= 1:
            new_achievements.append('approved_signal')
        
        # Check for 5-star rating
        has_five_star = db.signal_suggestions_collection.find_one({
            'suggested_by': user_id,
            'rating': 5
        })
        if 'five_star' not in current_achievements and has_five_star:
            new_achievements.append('five_star')
        
        # Elite status
        if 'elite' not in current_achievements and avg_rating >= 4.5 and signal_stats['approved'] >= 20:
            new_achievements.append('elite')
        
        # Award new achievements
        if new_achievements:
            db.users_collection.update_one(
                {'user_id': user_id},
                {'$addToSet': {'achievements': {'$each': new_achievements}}}
            )
            
            # Notify user
            for achievement_key in new_achievements:
                achievement = AchievementSystem.ACHIEVEMENTS[achievement_key]
                message = (
                    f"🎉 <b>Achievement Unlocked!</b>\n\n"
                    f"{achievement['name']}\n"
                    f"{achievement['description']}\n\n"
                    f"<b>Reward:</b> {achievement['reward']}"
                )
                
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
        
        return new_achievements

class ReferralSystem:
    """Reward users for inviting friends"""
    
    @staticmethod
    def generate_referral_link(user_id: int, bot_username: str) -> str:
        """Create unique referral link"""
        return f"https://t.me/{bot_username}?start=ref_{user_id}"
    
    @staticmethod
    async def process_referral(new_user_id: int, referrer_id: int, db, context):
        """Handle new user from referral"""
        
        # Award referrer
        db.users_collection.update_one(
            {'user_id': referrer_id},
            {
                '$inc': {'referrals': 1},
                '$push': {'referred_users': new_user_id}
            },
            upsert=True # Ensure referrer doc exists
        )
        
        # Check referral milestones
        referrer = db.users_collection.find_one({'user_id': referrer_id})
        referral_count = referrer.get('referrals', 0)
        
        rewards = {
            1: "🎁 +1 daily signal limit for 7 days",
            5: "🎁 +2 daily signal limit permanently",
            10: "💎 VIP status for 1 month",
            25: "🏆 Elite status + featured profile"
        }
        
        if referral_count in rewards:
            reward_message = (
                f"🎉 <b>Referral Milestone!</b>\n\n"
                f"You've referred {referral_count} users!\n\n"
                f"<b>Reward:</b> {rewards[referral_count]}\n\n"
                f"Keep sharing: /referral"
            )
            
            try:
                await context.bot.send_message(
                    chat_id=referrer_id,
                    text=reward_message,
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
        
        # Thank new user
        welcome_message = (
            f"👋 Welcome! You were referred by user {referrer_id}.\n\n"
            f"Both of you will earn rewards as you use PipSage!\n\n"
            f"Get started: /help"
        )
        
        try:
            await context.bot.send_message(
                chat_id=new_user_id,
                text=welcome_message
            )
        except:
            pass
    
    @staticmethod
    async def show_referral_stats(user_id: int, bot_username: str, db, update):
        """Display user's referral info"""
        
        user = db.users_collection.find_one({'user_id': user_id})
        referral_count = user.get('referrals', 0) if user else 0
        link = ReferralSystem.generate_referral_link(user_id, bot_username)
        
        # Calculate next milestone
        milestones = [1, 5, 10, 25, 50]
        next_milestone = next((m for m in milestones if m > referral_count), 50)
        
        message = (
            "🎁 <b>Your Referral Program</b>\n\n"
            
            f"📊 Total Referrals: {referral_count}\n"
            f"🎯 Next Milestone: {next_milestone} ({next_milestone - referral_count} more)\n\n"
            
            "<b>Your Unique Link:</b>\n"
            f"<code>{link}</code>\n\n"
            
            "<b>Rewards:</b>\n"
            "1 referral = +1 daily signal (7 days)\n"
            "5 referrals = +2 daily signal (permanent)\n"
            "10 referrals = VIP status (1 month)\n"
            "25 referrals = Elite status + feature\n\n"
            
            "💡 Share with friends who trade forex!"
        )
        
        keyboard = [[InlineKeyboardButton("📤 Share Link", url=f"https://t.me/share/url?url={link}&text=Check%20out%20this%20Forex%20Bot!")] ]
        
        await update.message.reply_text(
            message,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def show_testimonials_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display verified user testimonials"""
    
    testimonials = [
        {
            'user': 'John M.',
            'rating': 5,
            'text': 'PipSage signals helped me turn $500 into $2,400 in 3 months. The risk management tools are gold!',
            'verified': True
        },
        {
            'user': 'Sarah K.',
            'rating': 5,
            'text': 'Finally, a trading bot that\'s not spam! Real signals, real results. Worth every penny.',
            'verified': True
        },
        {
            'user': 'Mike T.',
            'rating': 4,
            'text': 'The educational content alone is worth it. Improved my win rate from 45% to 67%.',
            'verified': True
        }
    ]
    
    message = "⭐ <b>What Our Members Say</b>\n\n"
    
    for t in testimonials:
        stars = '⭐' * t['rating']
        verified = '✅ Verified' if t['verified'] else ''
        message += (
            f"{stars} {verified}\n"
            f"<i>\"{t['text']}\"</i>\n"
            f"— {t['user']}\n\n"
        )
    
    message += "Join them: /subscribe"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

class PromotionManager:
    """Handle special offers tastefully"""
    
    @staticmethod
    async def announce_promo(context: ContextTypes.DEFAULT_TYPE, db):
        """Announce promotion ONCE to active users"""
        
        # Only to users who haven't subscribed yet
        non_subscribers = db.users_collection.find({
            'user_id': {'$nin': list(db.get_all_subscribers())},
            'last_activity': {'$gte': time.time() - (7 * 86400)},  # Active in last 7 days
            'promo_nov_2024_seen': {'$ne': True}  # Haven't seen this promo
        })
        
        promo_message = (
            "🎁 <b>Special Offer - 7 Days Only</b>\n\n"
            
            "Join PipSage VIP and get:\n"
            "✅ First month 50% off\n"
            "✅ Bonus: 3 free private consultations\n"
            "✅ Lifetime access to tools\n\n"
            
            "Start: /subscribe\n\n"
            
            "<i>Expires: November 24, 2025</i>" # Updated year
        )
        
        sent = 0
        for user in non_subscribers:
            try:
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text=promo_message,
                    parse_mode=ParseMode.HTML
                )
                
                # Mark as seen
                db.users_collection.update_one(
                    {'user_id': user['user_id']},
                    {'$set': {'promo_nov_2024_seen': True}}
                )
                
                sent += 1
                await asyncio.sleep(0.1)
            except:
                pass
        
        logger.info(f"Promotion announced to {sent} active non-subscribers")

# ============================================
# NEW FEATURE: ANTI-SPAM & QUALITY CONTROL
# ============================================

class BroadcastFrequencyManager:
    """Prevent broadcast spam"""
    
    def __init__(self, db):
        self.db = db
        
    async def can_broadcast(self, admin_id: int) -> (bool, str):
        """Check if admin can send another broadcast"""
        
        # Get timestamp of the last broadcast
        last_broadcast = self.db.activity_logs_collection.find_one(
            {
                'user_id': admin_id,
                'action': {'$in': ['broadcast_sent', 'approved_broadcast_sent', 'broadcast_submitted']},
            },
            sort=[('timestamp', -1)]
        )
        
        last_broadcast_time = last_broadcast['timestamp'] if last_broadcast else 0
        
        # Limits per role
        role = self.db.get_admin_role(admin_id)
        limits_seconds = {
            AdminRole.SUPER_ADMIN: 30,  # 30 seconds
            AdminRole.ADMIN: 300,       # 5 minutes
            AdminRole.MODERATOR: 180,   # 3 minutes
            AdminRole.BROADCASTER: 600  # 10 minutes
        }
        
        limit = limits_seconds.get(role, 300) # Default 5 mins
        
        time_since_last = time.time() - last_broadcast_time
        
        if time_since_last < limit:
            time_remaining = limit - time_since_last
            return False, f"⏳ Broadcast limit reached. Try again in {int(time_remaining // 60)}m {int(time_remaining % 60)}s."
        
        return True, ""

class BroadcastQualityChecker:
    """Ensure broadcast quality"""
    
    @staticmethod
    def check_broadcast_quality(message_data: dict) -> (bool, list):
        """Validate broadcast before sending"""
        issues = []
        
        content = ""
        if message_data['type'] == 'text':
            content = message_data['content']
        elif message_data.get('caption'):
            content = message_data['caption']
        
        if not content:
             return True, [] # No text to check

        # Too short
        if len(content) < 10:
            issues.append("Message too short (minimum 10 characters)")
        
        # All caps (spam indicator)
        if content.isupper() and len(content) > 50:
            issues.append("Avoid ALL CAPS messages")
        
        # Too many emojis (basic check)
        emoji_count = 0
        for char in content:
            if char > '\u231a': # Simple check for emoji range
                emoji_count += 1
        
        if emoji_count > 15:
            issues.append("Too many emojis (max 15)")
        
        # Spam keywords
        spam_words = ['100% guaranteed', 'act fast', 'limited time only']
        if any(word.lower() in content.lower() for word in spam_words):
            issues.append("Message contains spam-like phrases (e.g., '100% guaranteed')")
        
        # Too many links
        link_count = content.lower().count('http')
        if link_count > 3:
            issues.append(f"Too many links ({link_count}). Max 3 per message.")
        
        return len(issues) == 0, issues

class UserEngagementTracker:
    """Track user interaction to personalize experience"""
    
    def __init__(self, db):
        self.db = db
    
    def update_engagement(self, user_id: int, action: str, value: int = 1):
        """Track user activity"""
        self.db.users_collection.update_one(
            {'user_id': user_id},
            {
                '$set': {'last_activity': time.time()},
                '$inc': {f'engagement.{action}': value}
            },
            upsert=True
        )
    
    def get_engagement_score(self, user_id: int) -> int:
        """Calculate engagement score (0-100)"""
        user = self.db.users_collection.find_one({'user_id': user_id})
        if not user:
            return 0
        
        engagement = user.get('engagement', {})
        
        # Weighted scoring
        score = (
            engagement.get('command_used', 0) * 2 +
            engagement.get('signal_suggested', 0) * 10 +
            engagement.get('signal_approved', 0) * 20 +
            engagement.get('vip_subscribed', 0) * 30
        )
        
        # Check recency
        last_activity = user.get('last_activity', 0)
        days_inactive = (time.time() - last_activity) / 86400
        
        if days_inactive > 30:
            score *= 0.5  # Decay for inactive users
        
        return min(int(score), 100)
    
    async def re_engage_inactive_users(self, context: ContextTypes.DEFAULT_TYPE):
        """Gentle re-engagement for inactive users"""
        
        # Find users inactive for 7+ days but less than 30
        cutoff_recent = time.time() - (7 * 86400)
        cutoff_old = time.time() - (30 * 86400)
        
        inactive_users = self.db.users_collection.find({
            'last_activity': {
                '$lt': cutoff_recent,
                '$gte': cutoff_old
            },
            're_engaged': {'$ne': True}
        })
        
        message = (
            "👋 Hey! We noticed you haven't checked in lately.\n\n"
            
            "Here's what you're missing:\n"
            "📊 New trading tools\n"
            "💡 Daily market insights\n"
            "🏆 Signal leaderboards\n\n"
            
            "Tap /start to see what's new!"
        )
        
        for user in inactive_users:
            try:
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text=message
                )
                
                # Mark as re-engaged (don't spam them again)
                self.db.users_collection.update_one(
                    {'user_id': user['user_id']},
                    {'$set': {'re_engaged': True}}
                )
                
                await asyncio.sleep(1)  # Slow rate for re-engagement
            except:
                pass

class NotificationManager:
    """Respect user preferences"""
    
    DEFAULT_PREFS = {
        'broadcasts': True, # General announcements
        'signals': True,    # Approved signals
        'leaderboards': True, # Weekly/Monthly leaderboards
        'tips': True,       # Daily tips
        'promo': True,      # Marketing promos
        'achievements': True # Achievement notifications
    }

    def __init__(self, db):
        self.db = db

    def get_notification_preferences(self, user_id: int) -> dict:
        """Get user's notification settings, applying defaults"""
        user = self.db.users_collection.find_one({'user_id': user_id})
        
        if not user or 'notifications' not in user:
            return self.DEFAULT_PREFS.copy()
        
        # Merge user prefs with defaults to ensure all keys exist
        user_prefs = user.get('notifications', {})
        prefs = self.DEFAULT_PREFS.copy()
        prefs.update(user_prefs) # Overwrite defaults with user's choices
        
        return prefs

    def should_notify(self, user_id: int, notification_type: str) -> bool:
        """Check if user wants this notification"""
        # Ensure type is valid
        if notification_type not in self.DEFAULT_PREFS:
            logger.warning(f"Invalid notification_type check: {notification_type}")
            return True # Default to sending if type is unknown

        prefs = self.get_notification_preferences(user_id)
        return prefs.get(notification_type, True)
        
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
        self.used_cr_numbers_collection = None  # New collection for CR numbers
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
            self.used_cr_numbers_collection = self.db['used_cr_numbers'] # New collection

            # Create indexes
            self.users_collection.create_index('user_id', unique=True)
            self.subscribers_collection.create_index('user_id', unique=True)
            self.admins_collection.create_index('user_id', unique=True)
            self.templates_collection.create_index('created_by')
            self.scheduled_broadcasts_collection.create_index('scheduled_time')
            self.activity_logs_collection.create_index([('timestamp', -1)])
            self.broadcast_approvals_collection.create_index('status')
            self.signal_suggestions_collection.create_index('status')
            self.used_cr_numbers_collection.create_index('cr_number', unique=True) # Index for CR numbers

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
                        'last_activity': time.time() # <-- MODIFIED THIS LINE
                    },
                    '$setOnInsert': {
                        'created_at': time.time(),
                        'achievements': [],
                        'referrals': 0,
                        'daily_tips_enabled': True,
                        'leaderboard_public': True
                    } # <-- ADDED 'setOnInsert' fields
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
            # Try to get admin name from users collection
            user_info = self.users_collection.find_one({'user_id': user_id})
            admin_name = str(user_id)
            if user_info:
                admin_name = user_info.get('first_name') or user_info.get('username') or str(user_id)

            self.admins_collection.update_one(
                {'user_id': user_id},
                {
                    '$set': {
                        'user_id': user_id,
                        'name': admin_name, # Store admin name
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
                'action': {'$in': ['broadcast_sent', 'approved_broadcast_sent']}
            })
            total_templates = self.templates_collection.count_documents({'created_by': user_id})
            total_scheduled = self.scheduled_broadcasts_collection.count_documents({
                'created_by': user_id
            })
            total_ratings = self.activity_logs_collection.count_documents({
                'user_id': user_id,
                'action': 'signal_approved'
            })

            return {
                'broadcasts': total_broadcasts,
                'templates': total_templates,
                'scheduled': total_scheduled,
                'ratings': total_ratings
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

    def update_suggestion_status(self, suggestion_id: str, status: str, reviewed_by: int,
                                 reason: str = None, rating: int = None):
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
            if rating is not None:
                update_data['rating'] = rating

            self.signal_suggestions_collection.update_one(
                {'_id': ObjectId(suggestion_id)},
                {'$set': update_data}
            )
            log_details = {'suggestion_id': suggestion_id}
            if rating:
                log_details['rating'] = rating
            self.log_activity(reviewed_by, f'signal_{status}', log_details)
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

    # New methods for CR Number checks
    def is_cr_number_used(self, cr_number: str) -> bool:
        """Check if a CR number has already been used for verification"""
        try:
            return self.used_cr_numbers_collection.find_one({'cr_number': cr_number}) is not None
        except Exception as e:
            logger.error(f"Error checking CR number {cr_number}: {e}")
            return False # Fail safe, but log error

    def mark_cr_number_as_used(self, cr_number: str, user_id: int):
        """Mark a CR number as used by a specific user"""
        try:
            self.used_cr_numbers_collection.insert_one({
                'cr_number': cr_number,
                'user_id': user_id,
                'used_at': time.time()
            })
            return True
        except Exception as e:
            logger.error(f"Error marking CR number {cr_number} as used: {e}")
            return False

    # New methods for Leaderboards
    def get_suggester_stats(self, time_frame: str) -> List[Dict]:
        """Get signal suggester stats for a given time frame (weekly/monthly)"""
        try:
            current_time = time.time()
            if time_frame == 'weekly':
                start_time = current_time - timedelta(days=7).total_seconds()
            elif time_frame == 'monthly':
                start_time = current_time - timedelta(days=30).total_seconds()
            else:
                return []

            pipeline = [
                {
                    '$match': {
                        'status': 'approved',
                        'rating': {'$exists': True},
                        'reviewed_at': {'$gte': start_time}
                    }
                },
                {
                    '$group': {
                        '_id': '$suggested_by',
                        'suggester_name': {'$first': '$suggester_name'},
                        'average_rating': {'$avg': '$rating'},
                        'signal_count': {'$sum': 1}
                    }
                },
                {
                    '$sort': {
                        'average_rating': -1,
                        'signal_count': -1
                    }
                },
                {
                    '$limit': 10  # Top 10
                }
            ]
            return list(self.signal_suggestions_collection.aggregate(pipeline))
        except Exception as e:
            logger.error(f"Error getting suggester stats: {e}")
            return []

    # In MongoDBHandler class

    def get_admin_performance_stats(self, time_frame: str) -> List[Dict]:
        """Get admin performance stats including Duty Consistency"""
        try:
            current_time = time.time()
            if time_frame == 'weekly':
                start_time = current_time - timedelta(days=7).total_seconds()
                date_filter = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
            elif time_frame == 'monthly':
                start_time = current_time - timedelta(days=30).total_seconds()
                date_filter = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
            else:
                return []

            pipeline = [
                { '$project': { 'user_id': '$user_id', 'admin_name': '$name' } },
                # 1. Lookup Activities (Existing)
                {
                    '$lookup': {
                        'from': 'activity_logs',
                        'let': {'admin_user_id': '$user_id'},
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {'$eq': ['$user_id', '$$admin_user_id']},
                                            {'$gte': ['$timestamp', start_time]}
                                        ]
                                    }
                                }
                            }
                        ],
                        'as': 'activities'
                    }
                },
                # 2. Lookup Duty Completions (NEW)
                {
                    '$lookup': {
                        'from': 'admin_duties',
                        'let': {'admin_user_id': '$user_id'},
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {
                                        '$and': [
                                            {'$eq': ['$admin_id', '$$admin_user_id']},
                                            {'$gte': ['$date', date_filter]},
                                            {'$eq': ['$completed', True]} # Only count completed duties
                                        ]
                                    }
                                }
                            }
                        ],
                        'as': 'completed_duties'
                    }
                },
                # 3. Calculate Scores
                {
                    '$project': {
                        'admin_name': '$admin_name',
                        'user_id': '$user_id',
                        'broadcasts': {
                            '$size': {'$filter': {'input': '$activities', 'as': 'a', 'cond': {'$in': ['$$a.action', ['broadcast_sent', 'approved_broadcast_sent']]}}}
                        },
                        'approvals': {
                            '$size': {'$filter': {'input': '$activities', 'as': 'a', 'cond': {'$in': ['$$a.action', ['broadcast_approved', 'signal_approved']]}}}
                        },
                        'duty_days': {'$size': '$completed_duties'}, # Count of days duties were completed
                    }
                },
                {
                    '$addFields': {
                        # Updated Score Formula: Activities + (Duty Days * 3)
                        # Giving significant weight to completing daily duties
                        'score': {
                            '$add': ['$broadcasts', '$approvals', {'$multiply': ['$duty_days', 3]}]
                        }
                    }
                },
                { '$sort': {'score': -1} }
            ]
            return list(self.admins_collection.aggregate(pipeline))
        except Exception as e:
            logger.error(f"Error getting admin performance stats: {e}")
            return []

    def get_user_suggestions_today(self, user_id: int) -> int:
        """Count user's suggestions since midnight UTC today"""
        try:
            # Get the start of today (midnight) in UTC
            # Note: dt_time and timezone should be imported from datetime at the top of your file
            # (which they already are)
            today_utc = datetime.now(timezone.utc).date()
            start_of_today_timestamp = datetime.combine(today_utc, dt_time(0, 0, tzinfo=timezone.utc)).timestamp()

            count = self.signal_suggestions_collection.count_documents({
                'suggested_by': user_id,
                'created_at': {'$gte': start_of_today_timestamp}
            })
            return count
        except Exception as e:
            logger.error(f"Error counting today's suggestions for {user_id}: {e}")
            return 0 # Fail safe

    def get_user_average_rating(self, user_id: int) -> float:
        """Get user's average rating from approved signals"""
        try:
            pipeline = [
                {
                    '$match': {
                        'suggested_by': user_id,
                        'status': 'approved',
                        'rating': {'$exists': True}
                    }
                },
                {
                    '$group': {
                        '_id': '$suggested_by',
                        'average_rating': {'$avg': '$rating'}
                    }
                }
            ]
            result = list(self.signal_suggestions_collection.aggregate(pipeline))
            if result:
                return result[0]['average_rating']
            return 0.0 # No rated signals
        except Exception as e:
            logger.error(f"Error getting user average rating for {user_id}: {e}")
            return 0.0 # Fail safe

    def get_user_signal_stats(self, user_id: int) -> Dict:
        """Get a user's signal suggestion stats"""
        try:
            total_suggestions = self.signal_suggestions_collection.count_documents({
                'suggested_by': user_id
            })
            approved_suggestions = self.signal_suggestions_collection.count_documents({
                'suggested_by': user_id,
                'status': 'approved'
            })
            
            approval_rate = 0.0
            if total_suggestions > 0:
                approval_rate = (approved_suggestions / total_suggestions) * 100
                
            return {
                'total': total_suggestions,
                'approved': approved_suggestions,
                'rate': approval_rate
            }
        except Exception as e:
            logger.error(f"Error getting user signal stats for {user_id}: {e}")
            return {'total': 0, 'approved': 0, 'rate': 0.0}

    def get_user_suggester_rank(self, user_id: int) -> (int, int):
        """Get a user's rank on the all-time suggester leaderboard"""
        try:
            # This pipeline ranks all users by average rating, then signal count
            pipeline = [
                {
                    '$match': {
                        'status': 'approved',
                        'rating': {'$exists': True}
                    }
                },
                {
                    '$group': {
                        '_id': '$suggested_by',
                        'average_rating': {'$avg': '$rating'},
                        'signal_count': {'$sum': 1}
                    }
                },
                {
                    '$sort': {
                        'average_rating': -1,
                        'signal_count': -1
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'users': {'$push': {'user_id': '$_id'}}
                    }
                }
            ]
            result = list(self.signal_suggestions_collection.aggregate(pipeline))
            
            if not result or 'users' not in result[0]:
                return 0, 0 # No ranked users

            total_ranked_users = len(result[0]['users'])
            
            try:
                # Find the user's 1-based index (rank)
                rank = [i for i, user in enumerate(result[0]['users']) if user['user_id'] == user_id][0] + 1
                return rank, total_ranked_users
            except IndexError:
                return 0, total_ranked_users # User is not ranked

        except Exception as e:
            logger.error(f"Error getting user suggester rank for {user_id}: {e}")
            return 0, 0

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

class EducationalContentManager:
    """Manages educational content from a Telegram database channel"""
    
    def __init__(self, db, channel_id: str):
        self.db = db
        self.channel_id = channel_id
        self.educational_content_collection = self.db['educational_content']
        
        # FIX: Use a composite unique index with an explicit name to avoid deployment conflicts
        self.educational_content_collection.create_index(
            [('message_id', 1), ('chat_id', 1)], 
            unique=True,
            name='educational_content_id_chat_unique',
            background=True # Prevents blocking on startup
        )
        
    async def process_and_save(self, message):
        """Extract content from a message and save to DB"""
        if not message:
            return False

        content_type = 'text'
        file_id = None
        text_content = None
        caption = message.caption

        if message.text:
            content_type = 'text'
            text_content = message.text
        elif message.photo:
            content_type = 'photo'
            file_id = message.photo[-1].file_id
        elif message.video:
            content_type = 'video'
            file_id = message.video.file_id
        elif message.document:
            content_type = 'document'
            file_id = message.document.file_id
        else:
            return False # Unsupported type

        entry = {
            'message_id': message.message_id,
            'chat_id': message.chat.id,
            'type': content_type,
            'content': text_content,
            'file_id': file_id,
            'caption': caption,
            'saved_at': time.time()
        }

        try:
            # Use update_one with upsert to prevent duplicates
            self.educational_content_collection.update_one(
                {'message_id': message.message_id, 'chat_id': message.chat.id},
                {'$set': entry},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error saving educational content: {e}")
            return False

    # NOTE: This function is kept for compatibility but warns about limitations
    async def fetch_and_store_content(self, context, limit: int = 100):
        logger.warning("Standard Bots cannot fetch history. Please forward messages to the bot or post new content to the channel to sync.")
        return 0
    
    async def get_random_content(self):
        """Get a random piece of content from the DB"""
        pipeline = [{'$sample': {'size': 1}}]
        result = list(self.educational_content_collection.aggregate(pipeline))
        return result[0] if result else None

    async def broadcast_random_content(self, context, target_users):
        """Broadcast random content to a list of users"""
        content = await self.get_random_content()
        if not content:
            return 0, 0

        success = 0
        failed = 0
        
        # Define the footer
        footer = "\n\n🔕 Disable: /settings then toggle off Daily Tips"
        
        for user_id in target_users:
            try:
                if content['type'] == 'text':
                    # Append footer to text
                    text_to_send = content['content'] + footer
                    await context.bot.send_message(chat_id=user_id, text=text_to_send)
                elif content['type'] == 'photo':
                    # Append footer to caption
                    caption_to_send = (content.get('caption') or '') + footer
                    await context.bot.send_photo(chat_id=user_id, photo=content['file_id'], caption=caption_to_send)
                elif content['type'] == 'video':
                    caption_to_send = (content.get('caption') or '') + footer
                    await context.bot.send_video(chat_id=user_id, video=content['file_id'], caption=caption_to_send)
                elif content['type'] == 'document':
                    caption_to_send = (content.get('caption') or '') + footer
                    await context.bot.send_document(chat_id=user_id, document=content['file_id'], caption=caption_to_send)
                success += 1
            except:
                failed += 1
                
        return success, failed

class AdminDutyManager:
    """Manages daily task assignments for admins"""
    
    # Define duty categories with specific tasks
    DUTY_CATEGORIES = {
        'signal_review': {
            'name': '💡 Signal Review Duty',
            'emoji': '💡',
            'tasks': [
                'Review all pending signal suggestions',
                'Provide detailed feedback on rejected signals',
                'Rate approved signals accurately (1-5 stars)',
                'Respond to user questions about signals'
            ],
            'target': 'Review at least 5 signals',
            'priority': 'high'
        },
        'broadcast_approval': {
            'name': '📢 Broadcast Approval Duty',
            'emoji': '📢',
            'tasks': [
                'Review pending broadcast approvals',
                'Check broadcast quality and content',
                'Approve/reject within 2 hours',
                'Provide clear rejection reasons if declining'
            ],
            'target': 'Process all pending approvals',
            'priority': 'high'
        },
        'user_engagement': {
            'name': '👥 User Engagement Duty',
            'emoji': '👥',
            'tasks': [
                'Respond to user queries in the group',
                'Welcome new subscribers personally',
                'Check for inactive users and re-engage them',
                'Share a motivational message in VIP group'
            ],
            'target': 'Engage with at least 10 users',
            'priority': 'medium'
        },
        'content_creation': {
            'name': '📝 Content Creation Duty',
            'emoji': '📝',
            'tasks': [
                'Create 1 educational post for the channel',
                'Share a trading tip or analysis',
                'Upload new content to education database',
                'Review and update bot templates'
            ],
            'target': 'Create 1 quality content piece',
            'priority': 'medium'
        },
        'quality_control': {
            'name': '🔍 Quality Control Duty',
            'emoji': '🔍',
            'tasks': [
                'Review broadcast quality from past 7 days',
                'Check for spam or low-quality content',
                'Verify VIP subscription requests',
                'Monitor admin activity logs'
            ],
            'target': 'Complete quality audit',
            'priority': 'low'
        },
        'analytics_reporting': {
            'name': '📊 Analytics & Reporting Duty',
            'emoji': '📊',
            'tasks': [
                'Review bot statistics and user growth',
                'Check signal performance metrics',
                'Identify trends in user engagement',
                'Prepare summary report for the team'
            ],
            'target': 'Generate daily report',
            'priority': 'low'
        },
        'community_moderation': {
            'name': '🛡️ Community Moderation Duty',
            'emoji': '🛡️',
            'tasks': [
                'Monitor VIP group for violations',
                'Handle user complaints and issues',
                'Check for spam or inappropriate content',
                'Update community guidelines if needed'
            ],
            'target': 'Maintain community standards',
            'priority': 'medium'
        }
    }
    
    def __init__(self, db):
        self.db = db
        self.admin_duties_collection = self.db['admin_duties']
        
        # Create indexes
        self.CONTINUOUS_DUTIES = ['signal_review', 'broadcast_approval', 'user_engagement', 'community_moderation']
        self.FINITE_TASKS = ['content_creation', 'quality_control', 'analytics_reporting']
        self.admin_duties_collection.create_index([('date', -1)])
        self.admin_duties_collection.create_index('admin_id')

    def credit_duty_for_action(self, admin_id: int, action: str) -> bool:
        """
        Give credit to admin's duty when they perform relevant actions.
        Called whenever an admin does work that counts toward duties.
        """
        date_key = self.get_date_key()
        
        # Map actions to duty categories
        action_to_duty = {
            'signal_approved': 'signal_review',
            'signal_rejected': 'signal_review',
            'broadcast_approved': 'broadcast_approval',
            'broadcast_rejected': 'broadcast_approval',
            'broadcast_sent': 'user_engagement',
            'create_template': 'content_creation',
            'vip_approved': 'user_engagement',
            'vip_declined': 'user_engagement',
        }
        
        duty_category = action_to_duty.get(action)
        
        if not duty_category:
            return False
        
        # Find if this admin has this duty today OR if ANY admin has this duty
        # We'll credit the action to ALL admins with this duty (Collaborative Credit)
        duties_to_credit = list(self.admin_duties_collection.find({
            'date': date_key,
            'duty_category': duty_category,
            'completed': False
        }))
        
        if not duties_to_credit:
            return False
        
        # Add action record to each matching duty
        for duty in duties_to_credit:
            self.admin_duties_collection.update_one(
                {'_id': duty['_id']},
                {
                    '$push': {
                        'actions_taken': {
                            'action': action,
                            'by_admin': admin_id,
                            'at': time.time()
                        }
                    },
                    '$inc': {'action_count': 1}
                }
            )
        
        logger.info(f"Credited {action} to {len(duties_to_credit)} admin(s) with {duty_category} duty")
        return True

    def _check_if_work_existed(self, duty_category: str, date_key: str) -> bool:
        """Check if there was work AND if it was handled by anyone"""
        try:
            date_obj = datetime.strptime(date_key, '%Y-%m-%d')
            start_timestamp = date_obj.replace(tzinfo=timezone.utc).timestamp()
            end_timestamp = start_timestamp + 86400
            
            if duty_category == 'signal_review':
                # Check if signals were submitted
                submitted = self.db['signal_suggestions'].count_documents({
                    'created_at': {'$gte': start_timestamp, '$lt': end_timestamp}
                })
                return submitted > 0
            
            elif duty_category == 'broadcast_approval':
                # Check if broadcasts needed approval
                submitted = self.db['broadcast_approvals'].count_documents({
                    'created_at': {'$gte': start_timestamp, '$lt': end_timestamp}
                })
                return submitted > 0
            
            # Other categories always have work (engagement, content, etc.)
            return True
        except Exception as e:
            logger.error(f"Error checking work existence: {e}")
            return True # Default to True to be safe

    def auto_complete_duties_with_no_work(self) -> Dict[str, Dict]:
        """
        At day's end (Midnight UTC):
        1. Auto-complete if NO work existed (e.g. no broadcasts to approve)
        2. Verify & Complete 'Continuous' duties if actions were recorded
        3. Fail duties where work existed but wasn't done
        """
        date_key = self.get_date_key()
        
        incomplete_duties = list(self.admin_duties_collection.find({
            'date': date_key,
            'completed': False
        }))
        
        results = {
            'auto_completed_no_work': {},
            'verified_complete': {}, # New category for work done & verified by system
            'left_incomplete': {}
        }
        
        for duty in incomplete_duties:
            duty_category = duty['duty_category']
            admin_id = duty['admin_id']
            admin_name = duty['admin_name']
            action_count = duty.get('action_count', 0)
            
            # Check if work existed in the system
            had_work = self._check_if_work_existed(duty_category, date_key)
            
            if not had_work:
                # Scenario 1: No work existed at all.
                # Mark as completed (Exempt)
                self.admin_duties_collection.update_one(
                    {'_id': duty['_id']},
                    {
                        '$set': {
                            'completed': True,
                            'auto_completed': True,
                            'auto_reason': 'no_work',
                            'completed_at': time.time(),
                            'completion_notes': 'System: No work was available today'
                        }
                    }
                )
                results['auto_completed_no_work'].setdefault(duty_category, []).append(admin_name)
            
            elif action_count > 0:
                # Scenario 2: Work existed AND this admin did actions.
                # This is a "System Verified Completion" (Good Job)
                self.admin_duties_collection.update_one(
                    {'_id': duty['_id']},
                    {
                        '$set': {
                            'completed': True,
                            'auto_completed': False, # It wasn't "auto" in a bad way, it was verified
                            'system_verified': True, # New flag
                            'completed_at': time.time(),
                            'completion_notes': f'System Verified: {action_count} actions recorded.'
                        }
                    }
                )
                results['verified_complete'].setdefault(duty_category, []).append(f"{admin_name} ({action_count} actions)")
                
            else:
                # Scenario 3: Work existed but admin did nothing (and didn't mark complete)
                results['left_incomplete'].setdefault(duty_category, []).append(admin_name)
        
        return results

    def get_completion_stats(self, days: int = 7) -> List[Dict]:
        """Get duty completion statistics with auto-complete breakdown"""
        start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')
        
        pipeline = [
            {'$match': {'date': {'$gte': start_date}}},
            {
                '$group': {
                    '_id': '$admin_id',
                    'admin_name': {'$first': '$admin_name'},
                    'total_duties': {'$sum': 1},
                    'completed_duties': {
                        '$sum': {'$cond': [{'$eq': ['$completed', True]}, 1, 0]}
                    },
                    # Count manually completed
                    'manual_completed': {
                        '$sum': {'$cond': [
                            {'$and': [
                                {'$eq': ['$completed', True]},
                                {'$ne': ['$auto_completed', True]}
                            ]}, 1, 0
                        ]}
                    },
                    'auto_completed': {
                        '$sum': {'$cond': [{'$eq': ['$auto_completed', True]}, 1, 0]}
                    }
                }
            },
            {
                '$project': {
                    'admin_id': '$_id',
                    'admin_name': 1,
                    'total_duties': 1,
                    'completed_duties': 1,
                    'manual_completed': 1,
                    'auto_completed': 1,
                    'completion_rate': {
                        '$multiply': [
                            {'$divide': ['$completed_duties', {'$max': ['$total_duties', 1]}]},
                            100
                        ]
                    }
                }
            },
            {'$sort': {'completion_rate': -1}}
        ]
        
        results = list(self.admin_duties_collection.aggregate(pipeline))
        return results
    
    def get_date_key(self) -> str:
        """Get today's date as a key"""
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    def assign_daily_duties(self, admin_list: List[Dict]) -> Dict[int, Dict]:
        """
        Assign duties to admins for the day using intelligent rotation.
        Returns dict of {admin_id: duty_info}
        """
        date_key = self.get_date_key()
        
        # Filter active admins (not broadcasters)
        eligible_admins = [
            admin for admin in admin_list 
            if admin['role'] in ['super_admin', 'admin', 'moderator']
        ]
        
        if not eligible_admins:
            logger.warning("No eligible admins for duty assignment")
            return {}
        
        # Get previous assignments to ensure rotation
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        yesterday_duties = list(self.admin_duties_collection.find({'date': yesterday}))
        
        # Create dict of {admin_id: yesterday_duty_category}
        last_assignments = {
            duty['admin_id']: duty['duty_category'] 
            for duty in yesterday_duties
        }
        
        # Get all duty categories
        duty_categories = list(self.DUTY_CATEGORIES.keys())
        
        # Shuffle for randomness
        random.shuffle(eligible_admins)
        random.shuffle(duty_categories)
        
        assignments = {}
        used_categories = set()
        
        # Assign duties ensuring:
        # 1. No admin gets same duty as yesterday
        # 2. High priority duties are covered
        # 3. Fair distribution
        
        # First pass: Assign high priority duties
        high_priority = [cat for cat, info in self.DUTY_CATEGORIES.items() if info['priority'] == 'high']
        
        for i, admin in enumerate(eligible_admins[:len(high_priority)]):
            admin_id = admin['user_id']
            last_duty = last_assignments.get(admin_id)
            
            # Find a high priority duty they didn't have yesterday
            assigned = False
            for category in high_priority:
                if category != last_duty and category not in used_categories:
                    assignments[admin_id] = {
                        'duty_category': category,
                        'duty_info': self.DUTY_CATEGORIES[category],
                        'admin_name': admin.get('name', str(admin_id)),
                        'admin_role': admin['role']
                    }
                    used_categories.add(category)
                    assigned = True
                    break
            
            # If all high priority were yesterday's, assign any high priority
            if not assigned:
                for category in high_priority:
                    if category not in used_categories:
                        assignments[admin_id] = {
                            'duty_category': category,
                            'duty_info': self.DUTY_CATEGORIES[category],
                            'admin_name': admin.get('name', str(admin_id)),
                            'admin_role': admin['role']
                        }
                        used_categories.add(category)
                        break
        
        # Second pass: Assign remaining duties to remaining admins
        remaining_admins = [a for a in eligible_admins if a['user_id'] not in assignments]
        remaining_duties = [cat for cat in duty_categories if cat not in used_categories]
        
        for i, admin in enumerate(remaining_admins):
            if i >= len(remaining_duties):
                break  # More admins than duties
            
            admin_id = admin['user_id']
            category = remaining_duties[i]
            
            assignments[admin_id] = {
                'duty_category': category,
                'duty_info': self.DUTY_CATEGORIES[category],
                'admin_name': admin.get('name', str(admin_id)),
                'admin_role': admin['role']
            }
        
        # Store assignments in database
        for admin_id, duty_data in assignments.items():
            self.admin_duties_collection.insert_one({
                'date': date_key,
                'admin_id': admin_id,
                'admin_name': duty_data['admin_name'],
                'admin_role': duty_data['admin_role'],
                'duty_category': duty_data['duty_category'],
                'duty_info': duty_data['duty_info'],
                'assigned_at': time.time(),
                'completed': False,
                'completion_notes': None
            })
        
        logger.info(f"Assigned {len(assignments)} duties for {date_key}")
        return assignments
    
    def mark_duty_complete(self, admin_id: int, notes: str = None) -> bool:
        """Mark today's duty as complete"""
        date_key = self.get_date_key()
        
        result = self.admin_duties_collection.update_one(
            {'date': date_key, 'admin_id': admin_id},
            {
                '$set': {
                    'completed': True,
                    'completed_at': time.time(),
                    'completion_notes': notes
                }
            }
        )
        
        return result.modified_count > 0
    
    def get_today_duty(self, admin_id: int) -> Optional[Dict]:
        """Get admin's duty for today"""
        date_key = self.get_date_key()
        return self.admin_duties_collection.find_one({'date': date_key, 'admin_id': admin_id})
    
    def get_completion_stats(self, days: int = 7) -> Dict:
        """Get duty completion statistics"""
        start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%d')
        
        pipeline = [
            {'$match': {'date': {'$gte': start_date}}},
            {
                '$group': {
                    '_id': '$admin_id',
                    'admin_name': {'$first': '$admin_name'},
                    'total_duties': {'$sum': 1},
                    'completed_duties': {
                        '$sum': {'$cond': [{'$eq': ['$completed', True]}, 1, 0]}
                    }
                }
            },
            {
                '$project': {
                    'admin_id': '$_id',
                    'admin_name': 1,
                    'total_duties': 1,
                    'completed_duties': 1,
                    'completion_rate': {
                        '$multiply': [
                            {'$divide': ['$completed_duties', '$total_duties']},
                            100
                        ]
                    }
                }
            },
            {'$sort': {'completion_rate': -1}}
        ]
        
        results = list(self.admin_duties_collection.aggregate(pipeline))
        return results
class TwitterIntegration:
    """Auto-post bot content to Twitter"""
    
    def __init__(self):
        self.api_key = os.getenv('TWITTER_API_KEY')
        self.api_secret = os.getenv('TWITTER_API_SECRET')
        self.access_token = os.getenv('TWITTER_ACCESS_TOKEN')
        self.access_secret = os.getenv('TWITTER_ACCESS_SECRET')
        
        if all([self.api_key, self.api_secret, self.access_token, self.access_secret]):
            auth = tweepy.OAuthHandler(self.api_key, self.api_secret)
            auth.set_access_token(self.access_token, self.access_secret)
            self.client = tweepy.Client(
                consumer_key=self.api_key,
                consumer_secret=self.api_secret,
                access_token=self.access_token,
                access_token_secret=self.access_secret
            )
            self.api = tweepy.API(auth)  # For media uploads
            logger.info("Twitter integration enabled")
        else:
            self.client = None
            self.api = None
            logger.warning("Twitter credentials not set")
    
    async def _upload_telegram_photo(self, context, file_id):
        """Download photo from Telegram and upload to Twitter"""
        try:
            # 1. Get file info from Telegram
            new_file = await context.bot.get_file(file_id)
            
            # 2. Download to memory buffer
            bio = io.BytesIO()
            await new_file.download_to_memory(bio)
            bio.seek(0)
            
            # 3. Upload to Twitter (v1.1 API)
            # Note: tweepy.API calls are synchronous, might block slightly but usually fine for images
            media = self.api.media_upload(filename="signal.jpg", file=bio)
            return [media.media_id]
        except Exception as e:
            logger.error(f"Error uploading photo to Twitter: {e}")
            return []

    async def post_signal(self, context, suggestion: Dict) -> Optional[str]:
        """Post approved signal to Twitter"""
        if not self.client:
            return None
        
        try:
            message_data = suggestion['message_data']
            suggester = suggestion['suggester_name']
            rating = suggestion.get('rating', 0)
            
            # Format tweet
            tweet_text = self._format_signal_tweet(message_data, suggester, rating)
            
            # Handle media
            media_ids = []
            if message_data['type'] == 'photo':
                media_ids = await self._upload_telegram_photo(context, message_data['file_id'])
            
            # Post tweet
            response = self.client.create_tweet(
                text=tweet_text,
                media_ids=media_ids if media_ids else None
            )
            
            tweet_url = f"https://twitter.com/user/status/{response.data['id']}"
            return tweet_url
            
        except Exception as e:
            logger.error(f"Failed to post to Twitter: {e}")
            return None
    
    def _format_signal_tweet(self, message_data: Dict, suggester: str, rating: int) -> str:
        """Format signal for Twitter (280 char limit)"""
        
        if message_data['type'] == 'text':
            content = message_data['content']
        else:
            content = message_data.get('caption') or "New Signal Alert"
        
        max_length = 200 
        if len(content) > max_length:
            content = content[:max_length] + "..."
        
        stars = "⭐" * rating if rating else ""
        
        tweet = f"💡 Trading Signal {stars}\n\n{content}\n\n👤 Signal by: {suggester}"
        return tweet[:280] 
    
    async def post_daily_tip(self, content: Dict) -> Optional[str]:
        """Post educational content"""
        if not self.client:
            return None
        
        try:
            if content['type'] == 'text':
                text_content = content['content']
            else:
                text_content = content.get('caption') or "Trading Tip"

            tweet_text = f"📚 Daily Trading Tip\n\n{text_content[:200]}\n\n#ForexEducation #TradingTips"
            
            response = self.client.create_tweet(text=tweet_text[:280])
            return f"https://twitter.com/user/status/{response.data['id']}"
            
        except Exception as e:
            logger.error(f"Failed to post tip to Twitter: {e}")
            return None
    
    async def post_performance_update(self, stats: Dict) -> Optional[str]:
        """Weekly performance transparency post"""
        if not self.client:
            return None
        
        try:
            # Extract stats safely
            total = stats.get('total_signals', 0)
            avg = stats.get('avg_rating', 0)
            excellent = stats.get('excellent_signals', 0)
            win_rate = (excellent / total * 100) if total > 0 else 0

            tweet = (
                f"📊 Weekly Performance Report\n\n"
                f"✅ Signals: {total}\n"
                f"⭐ Avg Rating: {avg:.1f}/5.0\n"
                f"🎯 Quality Rate: {win_rate:.1f}%\n\n"
                f"Transparent. Verified. Real.\n\n"
                f"#ForexSignals #TradingResults"
            )
            
            response = self.client.create_tweet(text=tweet)
            return f"https://twitter.com/user/status/{response.data['id']}"
            
        except Exception as e:
            logger.error(f"Failed to post performance: {e}")
            return None
            
class BroadcastBot:
    def __init__(self, token: str, super_admin_ids: List[int], mongo_handler: MongoDBHandler):
        self.token = token
        self.super_admin_ids = super_admin_ids
        self.db = mongo_handler
        self.watermarker = ImageWatermarker()
        
        # --- ADD THIS BLOCK ---
        self.engagement_tracker = UserEngagementTracker(self.db)
        self.broadcast_limiter = BroadcastFrequencyManager(self.db)
        self.notification_manager = NotificationManager(self.db)
        self.referral_system = ReferralSystem()
        self.achievement_system = AchievementSystem()
        self.twitter = TwitterIntegration()
        # ----------------------

        self.finnhub_client = None
        if FINNHUB_API_KEY:
            try:
                self.finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
                logger.info("Finnhub client initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to initialize Finnhub client: {e}")

        EDUCATION_CHANNEL_ID = os.getenv('EDUCATION_CHANNEL_ID')
        if EDUCATION_CHANNEL_ID:
            self.edu_content_manager = EducationalContentManager(
                self.db.db,  # Pass the database object
                EDUCATION_CHANNEL_ID
            )
            logger.info(f"Educational Content Manager initialized for channel: {EDUCATION_CHANNEL_ID}")
        else:
            self.edu_content_manager = None
            logger.warning("EDUCATION_CHANNEL_ID not set. Educational content feature disabled.")
        # Initialize Admin Duty Manager
        self.admin_duty_manager = AdminDutyManager(self.db.db)
        logger.info("Admin Duty Manager initialized")
        
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

    async def end_of_day_duty_verification_job(self, context: ContextTypes.DEFAULT_TYPE):
        """Runs at 23:55 UTC to auto-complete duties and send summary."""
        try:
            logger.info("Running end-of-day duty verification...")
            
            results = self.admin_duty_manager.auto_complete_duties_with_no_work()
            
            # Build comprehensive summary
            summary = "🤖 <b>End-of-Day Duty Report</b>\n"
            summary += f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
            
            # No work available
            if results['auto_completed_no_work']:
                summary += "✅ <b>Auto-Completed (No Work):</b>\n"
                for category, admins in results['auto_completed_no_work'].items():
                    duty_name = self.admin_duty_manager.DUTY_CATEGORIES[category]['emoji']
                    summary += f"{duty_name} {category.replace('_', ' ').title()}:\n"
                    for admin in admins:
                        summary += f"  • {admin}\n"
                summary += "\n"
            
            # Covered by team
            if results['auto_completed_covered']:
                summary += "🤝 <b>Auto-Completed (Team Coverage):</b>\n"
                for category, admins in results['auto_completed_covered'].items():
                    duty_name = self.admin_duty_manager.DUTY_CATEGORIES[category]['emoji']
                    summary += f"{duty_name} {category.replace('_', ' ').title()}:\n"
                    for admin in admins:
                        summary += f"  • {admin}\n"
                summary += "\n"
            
            # Incomplete (performance issue)
            if results['left_incomplete']:
                summary += "⚠️ <b>Incomplete (Work Not Done):</b>\n"
                for category, admins in results['left_incomplete'].items():
                    duty_name = self.admin_duty_manager.DUTY_CATEGORIES[category]['emoji']
                    summary += f"{duty_name} {category.replace('_', ' ').title()}:\n"
                    for admin in admins:
                        summary += f"  • {admin} ❌\n"
                summary += "\n"
            
            if not any(results.values()):
                summary += "✅ All duties completed manually. Great work team!\n"
            
            summary += "\n<i>Use /dutystats for detailed analytics</i>"
            
            # Notify super admins
            for super_admin_id in self.super_admin_ids:
                try:
                    await context.bot.send_message(
                        chat_id=super_admin_id,
                        text=summary,
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    logger.error(f"Failed to send end-of-day summary to {super_admin_id}: {e}")
            
        except Exception as e:
            logger.error(f"Error in end_of_day_duty_verification_job: {e}")

    async def is_user_subscribed(self, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Check if a user is subscribed to the force-sub channel"""
        FORCE_SUB_CHANNEL = os.getenv('FORCE_SUB_CHANNEL')
        if not FORCE_SUB_CHANNEL:
            return True # Skip check if not configured

        # Don't check admins
        if self.is_admin(user_id):
            return True

        try:
            member = await context.bot.get_chat_member(chat_id=FORCE_SUB_CHANNEL, user_id=user_id)
            if member.status in ['member', 'administrator', 'creator']:
                return True
            else:
                return False
        except BadRequest as e:
            if "user not found" in e.message or "chat not found" in e.message:
                # "user not found" means they aren't in the channel
                # "chat not found" means the FORCE_SUB_CHANNEL is wrong or bot isn't admin
                if "chat not found" in e.message:
                    logger.error(f"Force-sub error: Bot cannot access channel {FORCE_SUB_CHANNEL}. Is it an admin there?")
                return False
        except Exception as e:
            logger.error(f"Error in is_user_subscribed for {user_id}: {e}")
            return False # Fail-safe

    async def send_join_channel_message(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
        """Sends the 'please join' message"""
        FORCE_SUB_CHANNEL = os.getenv('FORCE_SUB_CHANNEL')
        if not FORCE_SUB_CHANNEL:
            return

        # Create a channel link from the username
        channel_link = f"https://t.me/{FORCE_SUB_CHANNEL.lstrip('@')}"

        keyboard = [
            [InlineKeyboardButton("Join Channel", url=channel_link)],
            [InlineKeyboardButton("I've Joined", callback_data="check_joined")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(
            chat_id=chat_id,
            text="You must join our updates channel to use this bot.\n\nPlease join the channel and then press 'I've Joined'.",
            reply_markup=reply_markup
        )

    async def check_joined_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles the 'I've Joined' button press"""
        query = update.callback_query
        user_id = query.from_user.id
        await query.answer("Checking...")

        if await self.is_user_subscribed(user_id, context):
            await query.edit_message_text("✅ Thank you! You can now use the bot.\n\nTry sending /start again.")
        else:
            await context.bot.send_message(
                chat_id=user_id,
                text="❌ You still haven't joined the channel. Please join and try again."
            )

        raise ApplicationHandlerStop
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

    def get_user_suggestion_limit(self, user_id: int) -> (int, str):
        """Determines a user's suggestion limit and level based on rating AND achievements"""
        
        # 1. Calculate Base Limit based on Rating
        avg_rating = self.db.get_user_average_rating(user_id)

        if avg_rating >= 4:
            base_limit = 5
            level = "Premium (4-5 Star)"
        elif avg_rating >= 3:
            base_limit = 2
            level = "Standard (3 Star)"
        else: # < 3 or 0
            base_limit = 1
            level = "Basic (0-2 Star)"
        bonus = 0
        user = self.db.users_collection.find_one({'user_id': user_id})
        
        if user and 'achievements' in user:
            achievements = user['achievements']
            if 'approved_signal' in achievements:
                bonus += 1
            if 'consistent' in achievements:
                bonus += 1
            if 'elite' in achievements:
                base_limit = 100
                level = "💎 Elite Trader"
        if user and 'referrals' in user:
            refs = user['referrals']
            if refs >= 5:
                bonus += 2

        total_limit = base_limit + bonus
        if bonus > 0 and base_limit < 50:
            level += f" (+{bonus} Bonus)"

        return total_limit, level

    async def start_v2(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Polished welcome with clear value prop & referral handling"""
        user = update.effective_user
        user_id = user.id
        
        # --- Referral Handling ---
        if context.args and context.args[0].startswith("ref_"):
            try:
                referrer_id = int(context.args[0].split('_')[1])
                if referrer_id != user_id:
                    # Check if user is new (no 'created_at' yet)
                    existing_user = self.db.users_collection.find_one({'user_id': user_id})
                    if not existing_user or 'created_at' not in existing_user:
                        # This is a new user, process referral
                        await self.referral_system.process_referral(user_id, referrer_id, self.db, context)
            except Exception as e:
                logger.error(f"Error processing referral: {e}")
        # -------------------------

        if not await self.is_user_subscribed(user_id, context):
            await self.send_join_channel_message(user_id, context)
            return

        self.db.add_user(user_id, user.username, user.first_name)
        self.engagement_tracker.update_engagement(user_id, 'command_used') # Track engagement
        
        # Check if new user (first time)
        user_doc = self.db.users_collection.find_one({'user_id': user_id})
        is_new = not user_doc.get('welcomed', False)

        if self.is_admin(user_id):
            # Admin Panel with inline buttons
            role = self.get_admin_role(user_id)
            admin_main_menu_text = (
                f"🔧 <b>Admin Panel</b> ({role.value.replace('_', ' ').title()})\n\n"
                "Welcome to the Admin Control Center. Select a category to manage."
            )

            keyboard = [
                [InlineKeyboardButton("📢 Broadcasting", callback_data='admin_broadcast')],
            ]
            if self.has_permission(user_id, Permission.APPROVE_BROADCASTS):
                keyboard.append([InlineKeyboardButton("✅ Approval System", callback_data='admin_approvals')])
            
            keyboard.append([InlineKeyboardButton("📝 Templates", callback_data='admin_templates')])
            
            # --- NEW: Team Duties & QA ---
            if self.is_admin(user_id):
                keyboard.append([InlineKeyboardButton("📋 Team Duties & QA", callback_data='admin_duties')])
                
            # --- NEW: Content & Education Management ---
            # Restricted to Super Admins (or role with permission to sync)
            if user_id in self.super_admin_ids: 
                 keyboard.append([InlineKeyboardButton("📚 Content & Education", callback_data='admin_content')])
            
            keyboard.append([InlineKeyboardButton("👥 User Management", callback_data='admin_users')])
            
            if self.has_permission(user_id, Permission.MANAGE_ADMINS):
                keyboard.append([InlineKeyboardButton("👨‍💼 Admin Management", callback_data='admin_admins')])
            
            if self.has_permission(user_id, Permission.VIEW_LOGS):
                keyboard.append([InlineKeyboardButton("📊 Monitoring", callback_data='admin_monitoring')])
            
            keyboard.append([InlineKeyboardButton("❓ Help", callback_data='admin_help')])

            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(admin_main_menu_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            if is_new:
                # Special welcome for new users
                welcome = (
                    f"👋 <b>Welcome to PipSage, {user.first_name}!</b>\n\n"
                    
                    "📈 We're a community of <b>serious traders</b> who:\n"
                    "• Share high-quality signals\n"
                    "• Learn risk management together\n"
                    "• Use powerful trading tools\n\n"
                    
                    "🎯 <b>Get Started:</b>\n"
                    "/subscribe - Join VIP for premium signals\n"
                    "/positionsize - Calculate lot size for risk\n"
                    "/settings - Manage your notifications\n"
                    "/help - Show all commands\n\n"
                    
                    "💡 <b>Become a Contributor:</b>\n"
                    "Earn status by sharing quality signals with /suggestsignal\n\n"
                    
                    "<i>Enable notifications to never miss important updates!</i>"
                )
                
                # Mark as welcomed
                self.db.users_collection.update_one(
                    {'user_id': user_id},
                    {'$set': {'welcomed': True}}
                )
            else:
                # Returning user
                welcome = (
                    f"Welcome back, {user.first_name}! 👋\n\n"
                    
                    "Quick access:\n"
                    "/mystats - Your performance\n"
                    "/myprogress - Your signal progress\n" # <-- NEW
                    "/referral - Refer friends\n" # <-- NEW
                    "/subscribe - VIP access\n"
                    "/help - All commands"
                )
            
            await update.message.reply_text(welcome, parse_mode=ParseMode.HTML)

    async def admin_button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if not self.is_admin(user_id):
            await query.edit_message_text("You are not authorized to use these commands.")
            return

        data = query.data
        message_text = ""
        keyboard = []
        commands = {
            'admin_broadcast': {
                'title': "📢 Broadcasting Commands",
                'description': "Manage sending messages and choosing optimal times.",
                'cmds': [
                    "/broadcast - Start broadcasting",
                    "/schedule - Schedule a broadcast",
                    "/scheduled - View scheduled broadcasts",
                    "/bestschedule - View optimal broadcast times",
                ]
            },
            'admin_approvals': {
                'title': "✅ Approval System Commands",
                'description': "Review and approve pending content.",
                'cmds': [
                    "/approvals - View pending approvals",
                    "/signals - View signal suggestions",
                ]
            },
            'admin_duties': {
                'title': "📋 Team Duties & QA",
                'description': "Manage daily admin tasks and monitor team performance.",
                'cmds': [
                    "/myduty - View your assigned task",
                    "/dutycomplete - Mark your task as complete",
                    "/dutystats - View team completion stats (Super Admins only)",
                ]
            },
            'admin_content': {
                'title': "📚 Content & Education Management",
                'description': "Manage the educational content database.",
                'cmds': [
                    "/synceducation - Manually sync content from channel (Super Admin only)",
                    "/previeweducation - Preview a random piece of content",
                ]
            },
            'admin_templates': {
                'title': "📝 Template Management",
                'description': "Create and manage message templates.",
                'cmds': [
                    "/templates - Manage templates",
                    "/savetemplate - Save current as template",
                ]
            },
            'admin_users': {
                'title': "👥 User Management Commands",
                'description': "Manage your bot's subscribers.",
                'cmds': [
                    "/add &lt;user_id&gt; - Add subscriber",
                    "/stats - View statistics",
                    "/subscribers - List subscribers",
                ]
            },
            'admin_admins': {
                'title': "👨‍💼 Admin Management Commands",
                'description': "Manage other administrators.",
                'cmds': [
                    "/addadmin - Add new admin",
                    "/removeadmin - Remove admin",
                    "/admins - List all admins",
                ]
            },
            'admin_monitoring': {
                'title': "📊 Monitoring & Analytics",
                'description': "Access logs and detailed performance metrics.",
                'cmds': [
                    "/logs - View activity logs (Super Admin only)",
                    "/mystats - Your individual performance statistics",
                ]
            },
            'admin_help': {
                'title': "❓ Admin Help",
                'description': "General information and assistance for admins.",
                'cmds': [
                    "Need specific help? Contact Executives.",
                ]
            }
        }

        if data in commands:
            category_info = commands[data]
            message_text = (
                f"<b>{category_info['title']}</b>\n\n"
                f"{category_info['description']}\n\n"
                f"<b>Commands:</b>\n"
                + "\n".join(category_info['cmds'])
            )
            keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Main", callback_data='admin_main_menu')])
        elif data == 'admin_main_menu':
            # Recreate the main admin menu
            role = self.get_admin_role(user_id)
            message_text = (
                f"🔧 <b>Admin Panel</b> ({role.value.replace('_', ' ').title()})\n\n"
                "Welcome to the Admin Control Center. Select a category to manage."
            )
            keyboard = [
                [InlineKeyboardButton("📢 Broadcasting", callback_data='admin_broadcast')],
            ]
            if self.has_permission(user_id, Permission.APPROVE_BROADCASTS):
                keyboard.append([InlineKeyboardButton("✅ Approval System", callback_data='admin_approvals')])
            
            keyboard.append([InlineKeyboardButton("📝 Templates", callback_data='admin_templates')])
            keyboard.append([InlineKeyboardButton("📋 Team Duties & QA", callback_data='admin_duties')])
                
            if user_id in self.super_admin_ids: 
                 keyboard.append([InlineKeyboardButton("📚 Content & Education", callback_data='admin_content')])
            keyboard.append([InlineKeyboardButton("👥 User Management", callback_data='admin_users')])
            
            if self.has_permission(user_id, Permission.MANAGE_ADMINS):
                keyboard.append([InlineKeyboardButton("👨‍💼 Admin Management", callback_data='admin_admins')])
            
            if self.has_permission(user_id, Permission.VIEW_LOGS):
                keyboard.append([InlineKeyboardButton("📊 Monitoring", callback_data='admin_monitoring')])
            
            keyboard.append([InlineKeyboardButton("❓ Help", callback_data='admin_help')])
        else:
            message_text = "Unknown admin command."
            keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Main", callback_data='admin_main_menu')])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

    async def help_command_v2(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Interactive help menu"""
        
        self.engagement_tracker.update_engagement(update.effective_user.id, 'command_used')
        
        keyboard = [
            [InlineKeyboardButton("📊 Trading Tools", callback_data="help_tools")],
            [InlineKeyboardButton("💎 VIP & Signals", callback_data="help_vip")],
            [InlineKeyboardButton("🏆 Community", callback_data="help_community")],
            [InlineKeyboardButton("⚙️ My Account", callback_data="help_account")]
        ]
        
        message = (
            "❓ <b>PipSage Help</b>\n\n"
            "What would you like to know about?"
        )
        
        await update.message.reply_text(
            message,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def handle_help_callbacks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show contextual help based on selection"""
        query = update.callback_query
        await query.answer()
        
        help_texts = {
            'help_tools': (
                "🛠 <b>Trading Tools</b>\n\n"
                "/pips - Calculate pip profit/loss\n"
                "/positionsize - Calculate lot size for risk\n"
                "/news - Latest forex news\n"
                "/calendar - Economic events\n\n"
                "💡 All tools work instantly!"
            ),
            'help_vip': (
                "💎 <b>VIP & Signals</b>\n\n"
                "/subscribe - Join VIP for premium signals\n"
                "/suggestsignal - Suggest a signal\n"
                "/performance - View our public stats\n"
                "/testimonials - See what members say\n\n"
                "Join: /subscribe"
            ),
            'help_community': (
                "🏆 <b>Community Features</b>\n\n"
                "/referral - Refer friends, earn rewards\n"
                "/mystats - View your signal stats\n"
                "/myprogress - Track your signal progress\n"
            ),
            'help_account': (
                "⚙️ <b>My Account</b>\n\n"
                "/settings - Manage notifications\n"
                "/start - View your main menu\n"
            ),
            'help_main': (
                "❓ <b>PipSage Help</b>\n\n"
                "What would you like to know about?"
            )
        }
        
        text = help_texts.get(query.data, "Coming soon!")
        
        # Build buttons
        if query.data == "help_main":
            keyboard = [
                [InlineKeyboardButton("📊 Trading Tools", callback_data="help_tools")],
                [InlineKeyboardButton("💎 VIP & Signals", callback_data="help_vip")],
                [InlineKeyboardButton("🏆 Community", callback_data="help_community")],
                [InlineKeyboardButton("⚙️ My Account", callback_data="help_account")]
            ]
        else:
            keyboard = [[InlineKeyboardButton("🔙 Back to Help", callback_data="help_main")]]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

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


    async def suggest_signal_start_v2(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enhanced signal suggestion with quality template"""
        user = update.effective_user
        user_id = user.id
        
        self.engagement_tracker.update_engagement(user_id, 'command_used')

        if not await self.is_user_subscribed(user_id, context):
            await self.send_join_channel_message(user_id, context)
            return ConversationHandler.END

        limit, level = self.get_user_suggestion_limit(user_id)
        today_count = self.db.get_user_suggestions_today(user_id)
        remaining = limit - today_count

        if remaining <= 0:
            # Show when limits reset + tips to improve rating
            avg_rating = self.db.get_user_average_rating(user_id)
            message = (
                f"❌ Daily limit reached ({today_count}/{limit})\n\n"
                f"📊 Your Stats:\n"
                f"Level: {level}\n"
                f"Avg Rating: {avg_rating:.1f}⭐\n\n"
            )
        
            if avg_rating < 3:
                message += (
                    "💡 <b>Improve your rating to unlock more signals:</b>\n"
                    "• Include clear entry/exit points\n"
                    "• Add stop loss and take profit\n"
                    "• Explain your reasoning\n"
                    "• Use proper pair format (e.g., EUR/USD)\n\n"
                )
        
            message += "⏰ Limits reset daily at 00:00 UTC"
        
            await update.message.reply_text(message, parse_mode=ParseMode.HTML)
            return ConversationHandler.END

        # Provide template for quality signals
        template = (
            "💡 <b>Submit a Quality Signal</b>\n\n"
            f"📊 Level: {level} ({remaining}/{limit} remaining)\n\n"
        
            "<b>Use this format for best results:</b>\n"
            "<code>PAIR: EUR/USD\n"
            "DIRECTION: BUY/SELL\n"
            "ENTRY: 1.0850\n"
            "SL: 1.0820 (-30 pips)\n"
            "TP1: 1.0900 (+50 pips)\n"
            "TP2: 1.0950 (+100 pips)\n"
            "REASON: (Why this trade?)</code>\n\n"
        
            "📸 Or send a clear screenshot\n"
            "⚠️ Low-quality signals may be rejected\n\n"
            "Send /cancel to cancel"
        )
    
        # The callback_data="show_signal_example" is correct and is handled by the fix in ConversationHandler
        keyboard = [[InlineKeyboardButton("📋 View Example", callback_data="show_signal_example")]]
    
        await update.message.reply_text(
            template,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return WAITING_SIGNAL_MESSAGE
    async def show_signal_example(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show example of good signal"""
        query = update.callback_query
        await query.answer()
    
        example = (
            "✅ <b>Example of 5-Star Signal</b>\n\n"
        
            "<code>PAIR: GBP/USD\n"
            "DIRECTION: BUY\n"
            "ENTRY: 1.2650-1.2670\n"
            "SL: 1.2600 (-60 pips)\n"
            "TP1: 1.2750 (+90 pips)\n"
            "TP2: 1.2850 (+190 pips)\n\n"
        
            "REASON:\n"
            "- Bullish divergence on 4H\n"
            "- Support at 1.2650\n"
            "- USD weakness ahead of FOMC\n"
            "- Risk/Reward: 1:3</code>\n\n"
        
            "Clear, specific, and well-reasoned! ⭐⭐⭐⭐⭐"
        )
    
        await query.edit_message_text(example, parse_mode=ParseMode.HTML)

    async def handle_force_submit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle forced submission after warning"""
        query = update.callback_query
        await query.answer()
    
        if query.data == "force_submit_text":
            text = context.user_data.get('pending_signal_text')
            if text:
                message_data = {
                    'type': 'text',
                    'content': text + "\n\n⚠️ [Low quality warning - submitted anyway]"
                }
            
                suggestion_id = self.db.create_signal_suggestion(
                    message_data,
                    query.from_user.id,
                    query.from_user.first_name or query.from_user.username or str(query.from_user.id)
                )
            
                if suggestion_id:
                    self.engagement_tracker.update_engagement(query.from_user.id, 'signal_suggested')
                    await self.achievement_system.check_and_award_achievements(query.from_user.id, context, self.db)
                    await query.edit_message_text(
                        "✅ Signal submitted!\n\n"
                        "⚠️ Note: Low-quality signals may receive lower ratings."
                    )
                    await self.notify_super_admins_new_suggestion(context, suggestion_id)
                else:
                    await query.edit_message_text("❌ Failed to submit.")
            return ConversationHandler.END
    
        elif query.data == "force_submit_photo":
            photo_data = context.user_data.get('pending_signal_photo')
            if photo_data:
                ocr_text = photo_data.get('ocr_text', '')
                caption_text = f"[Extracted Text]:\n{ocr_text[:500]}...\n\n⚠️ [Low quality warning - submitted anyway]" if ocr_text else "⚠️ [Low quality warning - submitted anyway]"
            
                message_data = {
                    'type': 'photo',
                    'file_id': photo_data['file_id'],
                    'caption': caption_text
                }
            
                suggestion_id = self.db.create_signal_suggestion(
                    message_data,
                    query.from_user.id,
                    query.from_user.first_name or query.from_user.username or str(query.from_user.id)
                )
            
                if suggestion_id:
                    self.engagement_tracker.update_engagement(query.from_user.id, 'signal_suggested')
                    await self.achievement_system.check_and_award_achievements(query.from_user.id, context, self.db)
                    await query.edit_message_text(
                        "✅ Signal submitted!\n\n"
                        "⚠️ Note: Low-quality images may receive lower ratings."
                    )
                    await self.notify_super_admins_new_suggestion(context, suggestion_id)
                else:
                    await query.edit_message_text("❌ Failed to submit.")
            return ConversationHandler.END
    
        elif query.data == "cancel_signal":
            await query.edit_message_text("❌ Cancelled. Send /suggestsignal to try again.")
            return ConversationHandler.END
            
    def validate_signal_format(self, text: str) -> (bool, str):
        """Check if signal meets minimum quality standards"""
        required_elements = ['pair', 'entry']
        text_lower = text.lower()
    
        missing = []
        for element in required_elements:
            if f"{element}:" not in text_lower and element not in text_lower:
                missing.append(element.upper())
    
        if missing:
            return False, f"Missing required fields: {', '.join(missing)}. Please include at least Pair and Entry."
    
        # TWEAK: Expand pair detection to include more formats
        pairs = ['EUR', 'USD', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF', 
                 'XAU', 'GOLD', 'SILVER', 'XAG', 'OIL', 'CRUDE',
                 'V25', 'V75', 'V100', 'BOOM', 'CRASH',  # Deriv indices
                 'US30', 'NAS100', 'SPX500']  # Added indices
        has_pair = any(pair in text.upper() for pair in pairs)
    
        # TWEAK: More flexible pair format matching
        if not has_pair:
            # Match XXX/YYY, XXXYYY, or common formats
            if not re.search(r'[A-Z]{3}[/\s]?[A-Z]{3}', text.upper()):
                return False, "Could not identify trading pair. Use format like 'EUR/USD', 'EURUSD' or 'GOLD'."
    
        # TWEAK: Add minimum length check instead of strict format
        if len(text.strip()) < 20:
            return False, "Signal description is too short. Please provide more details (entry, target, reasoning)."
    
        return True, "Valid"

    async def validate_signal_image(self, photo_file: 'telegram.PhotoSize') -> (bool, str, str):
        """Validate signal image - MORE LENIENT"""
        # TWEAK: Reduce minimum dimensions
        MIN_WIDTH = 200   # Was 300
        MIN_HEIGHT = 150  # Was 200

        if photo_file.width < MIN_WIDTH or photo_file.height < MIN_HEIGHT:
            return False, f"Image is too small ({photo_file.width}x{photo_file.height}). Minimum is {MIN_WIDTH}x{MIN_HEIGHT}px.", ""
    
        try:
            photo_bytes = await (await photo_file.get_file()).download_as_bytearray()
            image = Image.open(io.BytesIO(photo_bytes))
        
            image = image.convert('L')
        
            extracted_text = pytesseract.image_to_string(image)
        
            if not extracted_text or len(extracted_text.strip()) < 5:  # Was 10
                return False, "Image is unclear. Could not read any text from it.", ""
        
            if len(extracted_text.strip()) >= 15:  # Only validate longer texts
                is_valid, reason = self.validate_signal_format(extracted_text)
            
                if not is_valid:
                    return False, f"Image text is incomplete. {reason}", extracted_text
            else:
                # TWEAK: Accept image even if OCR is poor (admin will review)
                logger.info(f"Low OCR quality ({len(extracted_text)} chars), accepting for manual review")
        
            return True, "Image is valid", extracted_text
    
        except Exception as e:
            logger.error(f"Error processing signal image: {e}")
            return False, "Failed to process image. It might be in an unsupported format.", ""
    
    async def receive_signal_suggestion(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive signal suggestion with WARNING system instead of hard rejection"""
        user = update.effective_user
        message = update.message
    
        message_data = {
            'type': 'text',
            'content': None
        }

        if message.text:
            is_valid, reason = self.validate_signal_format(message.text)
            if not is_valid:
                # TWEAK: Warning instead of rejection
                keyboard = [
                    [InlineKeyboardButton("✅ Submit Anyway", callback_data=f"force_submit_text")],
                    [InlineKeyboardButton("❌ Cancel & Fix", callback_data="cancel_signal")]
                ]
                context.user_data['pending_signal_text'] = message.text
            
                await update.message.reply_text(
                    f"⚠️ <b>Quality Check Warning</b>\n\n"
                    f"<b>Issue:</b> {reason}\n\n"
                    f"You can still submit, but it may be rejected by admins.\n"
                    f"Tip: High-quality signals get better ratings!",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return ConversationHandler.END  # Wait for callback
        
            message_data['type'] = 'text'
            message_data['content'] = message.text

        elif message.photo:
            photo = message.photo[-1]
            message_data['type'] = 'photo'
            message_data['file_id'] = photo.file_id
            message_data['caption'] = message.caption

            if message.caption:
                is_valid, reason = self.validate_signal_format(message.caption)
                if not is_valid:
                    # TWEAK: Show warning but allow submission
                    await update.message.reply_text(
                        f"⚠️ Caption may be incomplete: {reason}\n\n"
                        f"✅ Submitting anyway since you included an image...",
                        parse_mode=ParseMode.HTML
                    )
            else: 
                is_valid, reason, ocr_text = await self.validate_signal_image(photo)
    
                if not is_valid and "too small" in reason.lower():
                    await update.message.reply_text(
                        f"❌ {reason}",
                        parse_mode=ParseMode.HTML
                    )
                    return ConversationHandler.END
                elif not is_valid:
                    keyboard = [
                        [InlineKeyboardButton("✅ Submit Anyway", callback_data="force_submit_photo")],
                        [InlineKeyboardButton("❌ Cancel & Fix", callback_data="cancel_signal")]
                    ]
                    context.user_data['pending_signal_photo'] = {
                        'file_id': photo.file_id,
                        'ocr_text': ocr_text
                    }
        
                    await update.message.reply_text(
                        f"⚠️ <b>Image Quality Warning</b>\n\n"
                        f"<b>Issue:</b> {reason}\n\n"
                        f"You can still submit, but it may be rejected by admins.\n"
                        f"Tip: Clear screenshots with visible text get better ratings!",
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                   )
                    return ConversationHandler.END
    
                if not message_data['caption'] and ocr_text:
                    message_data['caption'] = f"[Extracted Text]:\n{ocr_text[:500]}..."
        elif message.video:
            # We can't validate video, so we just accept it
            message_data['type'] = 'video'
            message_data['file_id'] = message.video.file_id
            message_data['caption'] = message.caption
        elif message.document:
             # We can't validate documents, so we just accept it
            message_data['type'] = 'document'
            message_data['file_id'] = message.document.file_id
            message_data['caption'] = message.caption
        else:
            await update.message.reply_text("Unsupported format. Please send text or a photo.")
            return ConversationHandler.END


        # --- IF ALL CHECKS PASS, SAVE ---
        suggestion_id = self.db.create_signal_suggestion(
            message_data,
            user.id,
            user.first_name or user.username or str(user.id)
        )

        if suggestion_id:
            self.engagement_tracker.update_engagement(user.id, 'signal_suggested')
            await self.achievement_system.check_and_award_achievements(user.id, context, self.db)
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
                InlineKeyboardButton("✅ Approve", callback_data=f"sig_approve_{suggestion_id}"),
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
            error_text = "❌ Only Super Admins can review suggestions."
            if query.message.text:
                await query.edit_message_text(text=error_text)
            elif query.message.caption:
                await query.edit_message_caption(caption=error_text)
            return ConversationHandler.END

        action, suggestion_id = query.data.split('_', 2)[1:]

        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if not suggestion:
            error_text = "❌ Suggestion not found."
            if query.message.text:
                await query.edit_message_text(text=error_text)
            elif query.message.caption:
                await query.edit_message_caption(caption=error_text)
            return ConversationHandler.END

        if action == "approve":
            # Store suggestion ID and ask for rating
            context.user_data['suggestion_to_rate'] = suggestion_id
            keyboard = [
                [
                    InlineKeyboardButton("⭐", callback_data="sig_rate_1"),
                    InlineKeyboardButton("⭐⭐", callback_data="sig_rate_2"),
                    InlineKeyboardButton("⭐⭐⭐", callback_data="sig_rate_3"),
                    InlineKeyboardButton("⭐⭐⭐⭐", callback_data="sig_rate_4"),
                    InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data="sig_rate_5"),
                ]
            ]
            # ...
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            new_prompt = "Please rate this signal (1-5 stars) before approving:"
            
            if query.message.text:
                await query.edit_message_text(
                    text=new_prompt,
                    reply_markup=reply_markup
                )
            elif query.message.caption:
                await query.edit_message_caption(
                    caption=new_prompt,
                    reply_markup=reply_markup
                )
            
            return WAITING_SIGNAL_RATING
            # ...

        elif action == "reject":
            # Store suggestion ID and ask for rejection reason
            context.user_data['suggestion_to_reject'] = suggestion_id
            
            new_prompt = "Please provide a reason for rejecting this signal:"
            
            if query.message.text:
                await query.edit_message_text(
                    text=new_prompt,
                    reply_markup=None # Remove buttons
                )
            elif query.message.caption:
                await query.edit_message_caption(
                    caption=new_prompt,
                    reply_markup=None # Remove buttons
                )
            
            return WAITING_SIGNAL_REJECTION_REASON

    async def receive_signal_rating(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive signal rating, approve, and broadcast"""
        query = update.callback_query
        await query.answer()

        rating = int(query.data.split('_')[-1])
        suggestion_id = context.user_data.pop('suggestion_to_rate', None)

        if not suggestion_id:
            error_text = "❌ Error: Suggestion ID not found. Please try again."
            if query.message.text:
                await query.edit_message_text(text=error_text)
            elif query.message.caption:
                await query.edit_message_caption(caption=error_text)
            return ConversationHandler.END

        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if not suggestion:
            error_text = "❌ Error: Suggestion not found."
            if query.message.text:
                await query.edit_message_text(text=error_text)
            elif query.message.caption:
                await query.edit_message_caption(caption=error_text)
            return ConversationHandler.END

        # Update status with rating
        self.db.update_suggestion_status(suggestion_id, 'approved', query.from_user.id, rating=rating)
        self.admin_duty_manager.credit_duty_for_action(query.from_user.id, 'signal_approved')
        
        # --- ADD THIS BLOCK ---
        suggester_id = suggestion['suggested_by']
        self.engagement_tracker.update_engagement(suggester_id, 'signal_approved')
        if rating == 5:
            self.engagement_tracker.update_engagement(suggester_id, 'signal_5_star')
        await self.achievement_system.check_and_award_achievements(suggester_id, context, self.db)

        # Get updated suggestion data (with rating)
        suggestion = self.db.get_suggestion_by_id(suggestion_id)

        # Broadcast to all users
        await self.broadcast_signal(context, suggestion)

        # Notify suggester
        try:
            await context.bot.send_message(
                chat_id=suggestion['suggested_by'],
                text=f"✅ Your signal suggestion has been approved with a rating of {rating} stars and broadcasted! Thank you for your contribution."
            )
        except:
            pass

        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"✅ Signal approved with {rating} stars and broadcasted to all users!")
        return ConversationHandler.END

    async def receive_signal_rejection_reason(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive signal rejection reason, update status, and notify suggester"""
        reason = update.message.text
        suggestion_id = context.user_data.pop('suggestion_to_reject', None)
        admin_user = update.effective_user

        if not suggestion_id:
            await update.message.reply_text("❌ Error: Suggestion ID not found. Please try again.")
            return ConversationHandler.END

        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if not suggestion:
            await update.message.reply_text("❌ Error: Suggestion not found.")
            return ConversationHandler.END

        # Update status with rejection reason
        self.db.update_suggestion_status(
            suggestion_id, 
            'rejected', 
            admin_user.id, 
            reason=reason
        )
        self.admin_duty_manager.credit_duty_for_action(update.effective_user.id, 'signal_rejected')

        # Notify suggester
        try:
            await context.bot.send_message(
                chat_id=suggestion['suggested_by'],
                text=f"❌ Your signal suggestion was not approved.\n\nReason: {reason}"
            )
        except Exception as e:
            logger.warning(f"Failed to notify suggester {suggestion['suggested_by']} of rejection: {e}")

        await update.message.reply_text(f"❌ Signal rejected and reason recorded.")
        return ConversationHandler.END

    async def broadcast_signal(self, context: ContextTypes.DEFAULT_TYPE, suggestion: Dict):
        """Broadcast approved signal to all users"""
        target_users = self.db.get_all_users()
        message_data = suggestion['message_data']
        suggester = suggestion['suggester_name']
        rating = suggestion.get('rating') # Get the rating

        # Add attribution to message
        attribution = f"\n\n💡 Signal suggested by: {suggester}"
        if rating:
            attribution += f"\n⭐ Admin Rating: {'⭐' * rating}"


        success_count = 0
        failed_count = 0

        for user_id in target_users:
            if not self.notification_manager.should_notify(user_id, 'signals'):
                failed_count += 1
                continue
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
        tweet_url = await self.twitter.post_signal(context, suggestion)
        if tweet_url:
            try:
                await context.bot.send_message(
                    chat_id=suggestion['suggested_by'],
                    text=f"🎉 Your signal was also shared on Twitter!\n{tweet_url}"
                )
            except:
                pass

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
            self.admin_duty_manager.credit_duty_for_action(query.from_user.id, 'broadcast_approved')

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
            self.admin_duty_manager.credit_duty_for_action(query.from_user.id, 'broadcast_rejected')

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
                                            
        footer = "\n\n🔕 Disable: /settings then toggle off Admin Signals & Announcements"

        for user_id in target_users:
            if not self.notification_manager.should_notify(user_id, 'broadcasts'):
                failed_count += 1
                continue
            try:
                if message_data['type'] == 'text':
                    # Append footer
                    text_to_send = message_data['content'] + footer
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=text_to_send,
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )
                elif message_data['type'] == 'photo':
                    # Append footer
                    caption_to_send = (message_data.get('caption') or '') + footer
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=message_data['file_id'],
                        caption=caption_to_send,
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )
                elif message_data['type'] == 'video':
                    # Append footer
                    caption_to_send = (message_data.get('caption') or '') + footer
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=message_data['file_id'],
                        caption=caption_to_send,
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )
                elif message_data['type'] == 'document':
                    # Append footer
                    caption_to_send = (message_data.get('caption') or '') + footer
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=message_data['file_id'],
                        caption=caption_to_send,
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )

                success_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send approved broadcast to {user_id}: {e}")
                failed_count += 1

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
        # Mark as scheduled flow
        context.user_data['scheduled'] = True 
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

        message = f"🎯 Choose Target Audience\n\n{stats_text}\n\nWho should receive this message?"

        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
        elif hasattr(update, 'message'):
            await update.message.reply_text(message, reply_markup=reply_markup)
        else:
            # Fallback for schedule flow
            await context.bot.send_message(chat_id=update.from_user.id, text=message, reply_markup=reply_markup)

        return WAITING_TARGET

    async def handle_target_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle target audience choice and send broadcast"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        if 'scheduled_time' in context.user_data:
            return await self.finalize_scheduled_broadcast(update, context)

        target_map = {
            'target_all': 'all',
            'target_subscribers': 'subscribers',
            'target_nonsubscribers': 'nonsubscribers',
            'target_admins': 'admins'
        }
        target = target_map.get(query.data, 'all')
    
        broadcast_message = context.user_data['broadcast_message']
        inline_buttons = context.user_data.get('inline_buttons')
        protect_content = context.user_data.get('protect_content', False)
        use_watermark = context.user_data.get('use_watermark', False)
        watermarked_image = context.user_data.get('watermarked_image')

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
            message_data['caption'] = broadcast_message.caption
            if use_watermark and watermarked_image:
                try:
                    sent_photo = await context.bot.send_photo(
                        chat_id=user_id,
                        photo=watermarked_image,
                        caption="Generating file_id..."
                    )
                    message_data['file_id'] = sent_photo.photo[-1].file_id
                    await sent_photo.delete() # Clean up
                except Exception as e:
                    logger.error(f"Failed to send/delete watermarked photo: {e}")
                    message_data['file_id'] = broadcast_message.photo[-1].file_id # Fallback
            else:
                message_data['file_id'] = broadcast_message.photo[-1].file_id
        elif broadcast_message.video:
            message_data['type'] = 'video'
            message_data['file_id'] = broadcast_message.video.file_id
            message_data['caption'] = broadcast_message.caption
        elif broadcast_message.document:
            message_data['type'] = 'document'
            message_data['file_id'] = broadcast_message.document.file_id
            message_data['caption'] = broadcast_message.caption
    
        is_quality, issues = BroadcastQualityChecker.check_broadcast_quality(message_data)
        if not is_quality:
            issues_text = "\n".join([f"• {issue}" for issue in issues])
            await query.edit_message_text(
                f"❌ Broadcast Quality Check Failed:\n\n{issues_text}\n\n"
                "Please /cancel and try again."
            )
            return ConversationHandler.END
        if self.needs_approval(user_id):
        
            can_send, reason = await self.broadcast_limiter.can_broadcast(user_id)
            if not can_send:
                await query.edit_message_text(reason)
                return ConversationHandler.END
            creator_name = query.from_user.first_name or query.from_user.username or str(user_id)
            approval_id = self.db.create_broadcast_approval(
                message_data, # Use the prepared dict
                user_id,
                creator_name,
                target
            )

            if approval_id:
                self.db.log_activity(user_id, 'broadcast_submitted', {'approval_id': approval_id})
                await query.edit_message_text(
                    "⏳ Broadcast submitted for approval!\n\n"
                    "Moderators/Super Admins will review your broadcast.\n"
                    "You'll be notified when it's reviewed."
                )
                await self.notify_approvers_new_broadcast(context, approval_id)
            else:
                await query.edit_message_text("❌ Failed to submit broadcast. Please try again.")

            return ConversationHandler.END
        can_send, reason = await self.broadcast_limiter.can_broadcast(user_id)
        if not can_send:
            await query.edit_message_text(reason)
            return ConversationHandler.END

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
        message = f"📢 Broadcasting to {audience_name} ({len(target_users)} users)..."
        await query.edit_message_text(message)
    
        success_count = 0
        failed_count = 0

        for user_id_to_send in target_users: 
            if not self.notification_manager.should_notify(user_id_to_send, 'broadcasts'):
                failed_count += 1
                continue
            try:
                if message_data['type'] == 'text':
                    await context.bot.send_message(
                        chat_id=user_id_to_send,
                        text=message_data['content'],
                        reply_markup=inline_buttons,
                        protect_content=protect_content
                    )
                elif message_data['type'] == 'photo':
                    await context.bot.send_photo(
                        chat_id=user_id_to_send,
                        photo=message_data['file_id'], # Use the processed file_id
                        caption=message_data.get('caption'),
                        reply_markup=inline_buttons,
                        protect_content=protect_content
                    )
                elif message_data['type'] == 'video':
                    await context.bot.send_video(
                        chat_id=user_id_to_send,
                        video=message_data['file_id'],
                        caption=message_data.get('caption'),
                        reply_markup=inline_buttons,
                        protect_content=protect_content
                    )
                elif message_data['type'] == 'document':
                    await context.bot.send_document(
                        chat_id=user_id_to_send,
                        document=message_data['file_id'],
                        caption=message_data.get('caption'),
                        reply_markup=inline_buttons,
                        protect_content=protect_content
                    )

                success_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send to {user_id_to_send}: {e}")
                failed_count += 1

        self.db.log_activity(user_id, 'broadcast_sent', {
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
        context.user_data.clear()
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
                    # Define the footer
                    footer = "\n\n🔕 Disable: /settings then toggle off Admin Signals & Announcements"

                    # Send messages
                    for user_id in target_users:
                        if not self.notification_manager.should_notify(user_id, 'broadcasts'):
                            failed_count += 1
                            continue
                        try:
                            if message_data['type'] == 'text':
                                # Append footer
                                text_to_send = message_data['content'] + footer
                                await context.bot.send_message(
                                    chat_id=user_id,
                                    text=text_to_send,
                                    reply_markup=message_data.get('inline_buttons'),
                                    protect_content=message_data.get('protect_content', False)
                                )
                            elif message_data['type'] == 'photo':
                                # Append footer
                                caption_to_send = (message_data.get('caption') or '') + footer
                                await context.bot.send_photo(
                                    chat_id=user_id,
                                    photo=message_data['file_id'],
                                    caption=caption_to_send,
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

    # New Leaderboard Methods
    async def broadcast_suggester_leaderboard_v2(self, context: ContextTypes.DEFAULT_TYPE, time_frame: str):
        """Professional, motivating leaderboard"""
        stats = self.db.get_suggester_stats(time_frame)

        if not stats:
            return

        period = "This Week" if time_frame == 'weekly' else "This Month"
        
        message = (
            f"🏆 <b>Top Signal Contributors - {period}</b>\n\n"
            f"Our community's best performers:\n\n"
        )
        
        medals = ["🥇", "🥈", "🥉"]
        
        for i, stat in enumerate(stats[:10]):
            rank_icon = medals[i] if i < 3 else f"<b>{i+1}.</b>"
            
            # Check privacy
            user_doc = self.db.users_collection.find_one({'user_id': stat['_id']})
            is_public = user_doc.get('leaderboard_public', True) if user_doc else True
            name = stat['suggester_name'] if is_public else "Anonymous"

            rating = stat['average_rating']
            count = stat['signal_count']
            
            tier = ""
            if rating >= 4.5: tier = "⭐ Elite"
            elif rating >= 4.0: tier = "💎 Expert"
            elif rating >= 3.5: tier = "🔷 Advanced"
            else: tier = "📊 Active"
            
            message += (
                f"{rank_icon} <b>{name}</b> ({tier})\n"
                f"    {rating:.1f}⭐ • {count} signals\n\n"
            )
        
        message += (
            "\n💡 <b>Want to climb the ranks?</b>\n"
            "• Submit high-quality signals\n"
            "• Include clear entry/exit points\n"
            "• Explain your analysis\n\n"
            "Use /suggestsignal to contribute!"
        )
        message += "\n\n🔕 Disable: /settings then toggle off Leaderboards"
        
        # Send to all users who have not opted out
        target_users = self.db.get_all_users()
        for user_id in target_users:
            if self.notification_manager.should_notify(user_id, 'leaderboards'):
                try:
                    await context.bot.send_message(
                        chat_id=user_id, 
                        text=message,
                        parse_mode=ParseMode.HTML
                    )
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.error(f"Failed to send leaderboard to {user_id}: {e}")
    # MODIFIED FUNCTION
    async def _get_admin_performance_comment(self, score: int) -> str:
        """Generate a brutally honest comment on admin performance based ONLY on score"""
        if score == 0:
            return "Comment: No activity recorded — This level of performance is unacceptable. You’re failing to meet even the minimum expectations. Either improve immediately or risk losing relevance in the team.." #

        if score > 15:
            activity_level = "Outstanding performance — you're carrying the team."
        elif score > 8:
            activity_level = "Strong activity — solid effort but still room to push harder."
        elif score > 3:
            activity_level = "Average effort — you’re doing the bare minimum."
        else: # score is 1-3
            activity_level = "Poor activity — your contribution is disappointing."

        return f"Comment: {activity_level}" # Only return the score-based comment

    # MODIFIED FUNCTION
    async def broadcast_admin_leaderboard_v2(self, context: ContextTypes.DEFAULT_TYPE, time_frame: str):
        """Professional admin performance board - private to admins"""
        stats = self.db.get_admin_performance_stats(time_frame)

        if not stats:
            return

        period = "Weekly" if time_frame == 'weekly' else "Monthly"
        
        message = (
            f"📊 <b>Admin Team Performance - {period}</b>\n\n"
            f"Great work, team! Here's our activity summary:\n\n"
        )
        
        for i, stat in enumerate(stats[:10]):
            name = stat.get('admin_name', f"Admin {stat['user_id']}")
            score = stat.get('score', 0)
            broadcasts = stat.get('broadcasts', 0)
            approvals = stat.get('approvals', 0)
            rejections = stat.get('rejections', 0)
            
            level = ""
            if score >= 20: level = "🔥 Exceptional"
            elif score >= 12: level = "⚡ High Impact"
            elif score >= 6: level = "✅ Active"
            elif score >= 3: level = "📊 Contributing"
            else: level = "💤 Low Activity"
            
            message += (
                f"<b>{i+1}. {name}</b> ({level})\n"
                f"    Score: {score} | Broadcasts: {broadcasts}\n"
                f"    Approvals: {approvals} | Rejections: {rejections}\n\n"
            )
        
        message += "\n💡 <b>Team Insights:</b>\n"
        
        avg_score = sum(s.get('score', 0) for s in stats) / len(stats) if stats else 0
        
        if avg_score >= 10:
            message += "• Excellent team engagement this period! 🎉\n"
        elif avg_score >= 5:
            message += "• Solid team performance. Keep it up! 💪\n"
        else:
            message += "• Let's increase our activity. Users depend on us! 📈\n"
        
        message += "\n<i>Remember: Quality over quantity. Every interaction matters.</i>"
        
        # Send ONLY to admins
        target_admins = self.db.get_all_admin_ids()
        for admin_id in target_admins:
            try:
                await context.bot.send_message(
                    chat_id=admin_id, 
                    text=message,
                    parse_mode=ParseMode.HTML
                )
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send admin leaderboard to {admin_id}: {e}")

    async def run_leaderboards_job_v2(self, context: ContextTypes.DEFAULT_TYPE):
        """Job to run weekly/monthly leaderboards"""
        logger.info("Running weekly leaderboard job...")
        today = datetime.now(timezone.utc)

        # 1. Weekly Suggester Leaderboard
        await self.broadcast_suggester_leaderboard_v2(context, 'weekly')
        
        # 2. Weekly Admin Leaderboard
        await self.broadcast_admin_leaderboard_v2(context, 'weekly')

        # 3. Monthly Leaderboards (if first Sunday of month)
        if today.day <= 7:
            await self.broadcast_suggester_leaderboard_v2(context, 'monthly')
            await self.broadcast_admin_leaderboard_v2(context, 'monthly')

    def calculate_next_time(self, current_time: float, repeat: str) -> float:
        """Calculate next scheduled time based on repeat pattern"""
        dt = datetime.fromtimestamp(current_time)

        if repeat == 'daily':
            next_dt = dt + timedelta(days=1)
        elif repeat == 'weekly':
            next_dt = dt + timedelta(weeks=1)
        elif repeat == 'monthly':
            # A simple approximation
            next_dt = dt + timedelta(days=30)
        else:
            # Should not happen if 'repeat' is 'once'
            next_dt = dt

        return next_dt.timestamp()

    async def sync_educational_content(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin command to sync educational content from channel"""
        user_id = update.effective_user.id
        
        if user_id not in self.super_admin_ids:
            await update.message.reply_text("❌ Only Super Admins can use this command.")
            return
        
        if not self.edu_content_manager:
            await update.message.reply_text("❌ Educational content feature is not configured.")
            return
        
        await update.message.reply_text("🔄 Syncing educational content from channel...")
        
        try:
            # Note: This requires bot to see messages. 
            # Standard Bot API limitations apply regarding fetching history.
            count = await self.edu_content_manager.fetch_and_store_content(context, limit=200)
            await update.message.reply_text(
                f"✅ Successfully synced {count} educational content items!\n\n"
                f"Total in database: {self.edu_content_manager.educational_content_collection.count_documents({})}"
            )
        except Exception as e:
            logger.error(f"Error syncing content: {e}")
            await update.message.reply_text(f"❌ Error syncing content: {str(e)}")

    async def preview_educational_content(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin command to preview a random educational content"""
        user_id = update.effective_user.id
        
        if not self.is_admin(user_id):
            await update.message.reply_text("❌ Only admins can use this command.")
            return
        
        if not self.edu_content_manager:
            await update.message.reply_text("❌ Educational content feature is not configured.")
            return
        
        # FIX: Added 'await' to resolve the RuntimeWarning
        content = await self.edu_content_manager.get_random_content()
        
        if not content:
            await update.message.reply_text("❌ No educational content available in database.")
            return
        
        try:
            await update.message.reply_text("📚 <b>Preview of Random Educational Content:</b>", parse_mode=ParseMode.HTML)
            
            if content['type'] == 'text':
                await update.message.reply_text(content['content'])
            elif content['type'] == 'photo':
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=content['file_id'],
                    caption=content.get('caption', '')
                )
            elif content['type'] == 'video':
                await context.bot.send_video(
                    chat_id=user_id,
                    video=content['file_id'],
                    caption=content.get('caption', '')
                )
            elif content['type'] == 'document':
                await context.bot.send_document(
                    chat_id=user_id,
                    document=content['file_id'],
                    caption=content.get('caption', '')
                )
        except Exception as e:
            await update.message.reply_text(f"❌ Error previewing content: {str(e)}")

    async def auto_sync_education_job(self, context: ContextTypes.DEFAULT_TYPE):
        """Job to auto-sync educational content"""
        if self.edu_content_manager:
            try:
                count = await self.edu_content_manager.fetch_and_store_content(context, limit=200)
                logger.info(f"Auto-synced {count} educational content items")
            except Exception as e:
                logger.error(f"Error in auto-sync education job: {e}")

    async def assign_daily_duties_job(self, context: ContextTypes.DEFAULT_TYPE):
        """Job to assign daily duties at midnight UTC"""
        try:
            # Get all admins
            admins = self.db.get_all_admins()
            
            if not admins:
                logger.warning("No admins found for duty assignment")
                return
            
            # Assign duties
            assignments = self.admin_duty_manager.assign_daily_duties(admins)
            
            if not assignments:
                logger.warning("No duty assignments created")
                return
            
            # Send duty notifications to each admin
            for admin_id, duty_data in assignments.items():
                await self.send_duty_notification(context, admin_id, duty_data)
            
            # Send summary to super admins
            await self.send_duty_summary_to_super_admins(context, assignments)
            
            logger.info(f"Daily duties assigned and notifications sent to {len(assignments)} admins")
            
        except Exception as e:
            logger.error(f"Error in assign_daily_duties_job: {e}")
    
    async def send_duty_notification(self, context: ContextTypes.DEFAULT_TYPE, 
                                     admin_id: int, duty_data: Dict):
        """Send duty assignment notification to admin"""
        duty_info = duty_data['duty_info']
        
        # Format task list
        tasks_text = "\n".join([f"  • {task}" for task in duty_info['tasks']])
        
        # Determine priority emoji
        priority_emoji = {
            'high': '🔴',
            'medium': '🟡',
            'low': '🟢'
        }[duty_info['priority']]
        
        message = (
            f"{duty_info['emoji']} <b>Your Duty for Today</b>\n"
            f"{priority_emoji} Priority: {duty_info['priority'].upper()}\n\n"
            
            f"<b>{duty_info['name']}</b>\n\n"
            
            f"<b>Tasks:</b>\n{tasks_text}\n\n"
            
            f"<b>Target:</b> {duty_info['target']}\n\n"
            
            f"📝 When done, use: /dutycomplete [notes]\n"
            f"📋 View your duty: /myduty\n\n"
            
            f"<i>Let's keep the team productive! 💪</i>"
        )
        
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=message,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to send duty notification to {admin_id}: {e}")
    
    async def send_duty_summary_to_super_admins(self, context: ContextTypes.DEFAULT_TYPE, 
                                                assignments: Dict):
        """Send duty summary to super admins"""
        summary = (
            "📋 <b>Daily Duty Assignments Summary</b>\n"
            f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
        )
        
        for admin_id, duty_data in assignments.items():
            summary += (
                f"• <b>{duty_data['admin_name']}</b> ({duty_data['admin_role']})\n"
                f"  → {duty_data['duty_info']['emoji']} {duty_data['duty_category'].replace('_', ' ').title()}\n"
            )
        
        summary += "\n<i>Use /dutystats to view completion rates</i>"
        
        for super_admin_id in self.super_admin_ids:
            try:
                await context.bot.send_message(
                    chat_id=super_admin_id,
                    text=summary,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Failed to send duty summary to super admin {super_admin_id}: {e}")
    
    async def my_duty_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show admin's duty for today"""
        user_id = update.effective_user.id
        
        if not self.is_admin(user_id):
            await update.message.reply_text("❌ This command is for admins only.")
            return
        
        duty = self.admin_duty_manager.get_today_duty(user_id)
        
        if not duty:
            await update.message.reply_text(
                "📋 You don't have an assigned duty for today.\n\n"
                "<i>Duties are assigned daily at midnight UTC.</i>",
                parse_mode=ParseMode.HTML
            )
            return
        
        duty_info = duty['duty_info']
        tasks_text = "\n".join([f"  • {task}" for task in duty_info['tasks']])
        
        status = "✅ COMPLETED" if duty.get('completed') else "⏳ PENDING"
        
        message = (
            f"{duty_info['emoji']} <b>Your Duty for Today</b>\n"
            f"Status: {status}\n\n"
            
            f"<b>{duty_info['name']}</b>\n\n"
            
            f"<b>Tasks:</b>\n{tasks_text}\n\n"
            
            f"<b>Target:</b> {duty_info['target']}\n\n"
        )
        
        if duty.get('completed'):
            completed_at = datetime.fromtimestamp(duty['completed_at']).strftime('%H:%M UTC')
            message += f"Completed at: {completed_at}\n"
            if duty.get('completion_notes'):
                message += f"Notes: {duty['completion_notes']}\n"
        else:
            message += f"📝 Mark complete: /dutycomplete [notes]"
        
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
    
    async def duty_complete_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Mark duty as complete - WITH STRICT VERIFICATION"""
        user_id = update.effective_user.id
        
        if not self.is_admin(user_id):
            await update.message.reply_text("❌ This command is for admins only.")
            return
        
        duty = self.admin_duty_manager.get_today_duty(user_id)
        if not duty:
            await update.message.reply_text("❌ You don't have a duty assigned for today.")
            return
        
        if duty.get('completed'):
            await update.message.reply_text("✅ This duty is already marked as complete.")
            return

        category = duty['duty_category']
        action_count = duty.get('action_count', 0)

        # 1. Check if this is a Continuous Duty (Cannot be marked manually)
        if category in self.admin_duty_manager.CONTINUOUS_DUTIES:
            await update.message.reply_text(
                f"⚠️ <b>Cannot Mark Complete Manually</b>\n\n"
                f"The duty <b>{duty['duty_info']['name']}</b> is a continuous daily responsibility.\n\n"
                f"📊 <b>Current Activity:</b> {action_count} actions recorded.\n\n"
                f"✅ <b>How it works:</b>\n"
                f"Simply perform your tasks (approve signals, moderate, etc.). The system will automatically verify and mark this as 'Complete' at midnight UTC based on your logs.\n"
                f"You do not need to use this command.",
                parse_mode=ParseMode.HTML
            )
            return

        # 2. For Finite Tasks (Content Creation, etc.), verify work if possible
        # If it's a trackable finite task and 0 actions, warn them.
        if category == 'content_creation' and action_count == 0:
            # Allow them to bypass if they provide a link/note, otherwise block
            if not context.args:
                await update.message.reply_text(
                    f"⚠️ <b>Verification Required</b>\n\n"
                    f"The system hasn't detected any template creations or content syncs today.\n\n"
                    f"If you created content externally (e.g. directly in the channel), please provide proof/notes:\n"
                    f"<code>/dutycomplete Posted market update in channel</code>",
                    parse_mode=ParseMode.HTML
                )
                return

        # Proceed with manual completion for Finite Tasks
        notes = ' '.join(context.args) if context.args else None
        success = self.admin_duty_manager.mark_duty_complete(user_id, notes)
        
        if success:
            self.engagement_tracker.update_engagement(user_id, 'duty_completed')
            await update.message.reply_text(
                f"✅ <b>Duty Marked Complete!</b>\n\n"
                f"{duty['duty_info']['name']}\n"
                f"Notes: {notes or 'None'}",
                parse_mode=ParseMode.HTML
            )
            
            for super_admin_id in self.super_admin_ids:
                if super_admin_id != user_id:
                    try:
                        await context.bot.send_message(
                            chat_id=super_admin_id,
                            text=(
                                f"✅ <b>{duty['admin_name']}</b> completed their duty:\n"
                                f"{duty['duty_info']['emoji']} {duty['duty_info']['name']}\n"
                                + (f"\nNotes: {notes}" if notes else "")
                            ),
                            parse_mode=ParseMode.HTML
                        )
                    except:
                        pass
        else:
            await update.message.reply_text("❌ Failed to mark duty as complete. Please try again.")
    
    async def duty_stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show duty completion statistics with team collaboration"""
        user_id = update.effective_user.id
        
        if user_id not in self.super_admin_ids:
            await update.message.reply_text("❌ Only Super Admins can view duty statistics.")
            return
        
        stats = self.admin_duty_manager.get_completion_stats(days=7)
        
        if not stats:
            await update.message.reply_text("📊 No duty completion data available yet.")
            return
        
        message = "📊 <b>Duty Completion Stats (Last 7 Days)</b>\n\n"
        
        for stat in stats:
            completion_rate = stat['completion_rate']
            manual = stat.get('manual_completed', 0)
            auto = stat.get('auto_completed', 0)
            total = stat['total_duties']
            
            if completion_rate >= 80: status = "🟢"
            elif completion_rate >= 50: status = "🟡"
            else: status = "🔴"
            
            message += (
                f"{status} <b>{stat['admin_name']}</b>\n"
                f"   Total: {stat['completed_duties']}/{total} ({completion_rate:.1f}%)\n"
                f"   Manual: {manual} | Team-covered: {auto}\n\n"
            )
        
        message += (
            "\n<i>💡 'Team-covered' = work done by other admins when this admin was unavailable</i>\n\n"
            "Use /myduty to check your current duty"
        )
        
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    async def send_duty_reminders_job(self, context: ContextTypes.DEFAULT_TYPE):
        """Send reminders to admins with incomplete duties"""
        date_key = self.admin_duty_manager.get_date_key()
        
        # Find incomplete duties for today
        incomplete_duties = self.admin_duty_manager.admin_duties_collection.find({
            'date': date_key,
            'completed': False
        })
        
        for duty in incomplete_duties:
            admin_id = duty['admin_id']
            duty_info = duty['duty_info']
            
            message = (
                f"⏰ <b>Duty Reminder</b>\n\n"
                f"You have an incomplete duty:\n"
                f"{duty_info['emoji']} {duty_info['name']}\n\n"
                f"<b>Target:</b> {duty_info['target']}\n\n"
                f"Please complete it before end of day.\n"
                f"Mark done: /dutycomplete [notes]"
            )
            
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=message,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Failed to send duty reminder to {admin_id}: {e}")

    async def channel_post_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Auto-save new posts from the educational channel"""
        # Verify it's the correct channel
        # Note: self.edu_content_manager.channel_id might be a string like "-1001234567890"
        if str(update.effective_chat.id) == str(self.edu_content_manager.channel_id):
            saved = await self.edu_content_manager.process_and_save(update.channel_post)
            if saved:
                logger.info(f"Saved new educational content: {update.channel_post.message_id}")

    async def forward_listener(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Allow admins to forward old posts to backfill the database"""
        user_id = update.effective_user.id
        if not self.is_admin(user_id):
            return

        # Check if it's a forward
        if update.message.forward_from_chat:
             # You can verify the chat ID here if you want strict control
             saved = await self.edu_content_manager.process_and_save(update.message)
             if saved:
                 await update.message.reply_text("✅ Content saved to educational database!")

  
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
            entry_points=[CommandHandler("suggestsignal", self.suggest_signal_start_v2)],
            states={
                WAITING_SIGNAL_MESSAGE: [
                    CallbackQueryHandler(self.show_signal_example, pattern="^show_signal_example$"),
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

        vip_request_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.handle_vip_request_review, pattern="^vip_")],
            states={
                WAITING_DECLINE_REASON: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_decline_reason)
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

        # New handler for signal reviews (with rating)
        signal_review_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.handle_signal_review, pattern=r"^sig_(approve|reject)_")],
            states={
                WAITING_SIGNAL_RATING: [
                    CallbackQueryHandler(self.receive_signal_rating, pattern=r"^sig_rate_")
                ],
                # --- ADD THIS STATE ---
                WAITING_SIGNAL_REJECTION_REASON: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_signal_rejection_reason)
                ]
                # ---------------------
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)],
            conversation_timeout=300 # 5 minutes to rate
        )

       # Basic commands (REPLACE start and help)
        application.add_handler(CommandHandler("start", self.start_v2)) # <-- MODIFIED
        application.add_handler(CommandHandler("help", self.help_command_v2)) # <-- MODIFIED
        application.add_handler(CallbackQueryHandler(self.handle_help_callbacks, pattern="^help_")) # <-- NEW
        application.add_handler(CallbackQueryHandler(self.check_joined_callback, pattern="^check_joined$"))
        application.add_handler(CommandHandler("unsubscribe", self.unsubscribe))
        application.add_handler(CommandHandler("add", self.add_subscriber_command))
        application.add_handler(CommandHandler("stats", self.stats))
        application.add_handler(CommandHandler("subscribers", self.list_subscribers))

        # Approval system
        application.add_handler(CommandHandler("approvals", self.list_approvals))
        application.add_handler(CommandHandler("signals", self.list_signal_suggestions))
        application.add_handler(CallbackQueryHandler(self.handle_approval_review, pattern="^app_"))
        application.add_handler(signal_review_handler) # (Existing)

        # Admin management
        application.add_handler(add_admin_handler)
        application.add_handler(CommandHandler("removeadmin", self.remove_admin_command))
        application.add_handler(CommandHandler("admins", self.list_admins))
        application.add_handler(CommandHandler("logs", self.view_logs))
        application.add_handler(CommandHandler("mystats", self.my_stats))

        # --- NEW: Marketing & UX Commands ---
        application.add_handler(CommandHandler("performance", self.show_performance_command)) # <-- NEW
        application.add_handler(CommandHandler("referral", self.show_referral_command)) # <-- NEW
        application.add_handler(CommandHandler("testimonials", show_testimonials_command)) # <-- NEW
        application.add_handler(CommandHandler("myprogress", self.my_progress_command)) # <-- NEW
        application.add_handler(CommandHandler("settings", self.settings_command)) # <-- NEW
        application.add_handler(CallbackQueryHandler(self.handle_settings_callback, pattern="^toggle_")) # <-- NEW
        application.add_handler(CallbackQueryHandler(self.handle_settings_callback, pattern="^close_settings$")) # <-- NEW
        application.add_handler(CallbackQueryHandler(self.admin_button_handler, pattern='^admin_'))

        # --- Forex Toolkit Handlers (REPLACE pips) ---
        application.add_handler(CommandHandler("news", self.news))
        application.add_handler(CommandHandler("calendar", self.calendar))
        application.add_handler(CommandHandler("pips", self.pips_calculator_v2)) # <-- MODIFIED
        application.add_handler(CommandHandler("positionsize", self.position_size_calculator))
        application.add_handler(CommandHandler("bestschedule", self.suggest_broadcast_time)) # <-- NEW

        # Educational content management
        application.add_handler(CommandHandler("synceducation", self.sync_educational_content))
        application.add_handler(CommandHandler("previeweducation", self.preview_educational_content))
        # ----------------------------

        # Admin duty management
        application.add_handler(CommandHandler("myduty", self.my_duty_command))
        application.add_handler(CommandHandler("dutycomplete", self.duty_complete_command))
        application.add_handler(CommandHandler("dutystats", self.duty_stats_command))
        #
        
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
        application.add_handler(CallbackQueryHandler(self.handle_force_submit, pattern="^(force_submit_text|force_submit_photo|cancel_signal)$"))

        # Subscription and VIP request handling
        application.add_handler(subscribe_handler)
        application.add_handler(vip_request_handler)
        application.add_handler(
            MessageHandler(
                filters.Regex(r"^(Hello|Hi|Hey|Good morning|Good afternoon|Good evening|What's up|Howdy|Greetings|Hey there)$"),
                self.handle_greeting,
            )
        )

        if self.edu_content_manager:
            # 1. Listen for NEW posts in the channel
            # Ensure your .env EDUCATION_CHANNEL_ID matches the channel ID exactly (e.g., -100...)
            try:
                channel_id_int = int(self.edu_content_manager.channel_id)
                application.add_handler(
                    MessageHandler(
                        filters.Chat(chat_id=channel_id_int) & filters.UpdateType.CHANNEL_POST,
                        self.channel_post_handler
                    )
                )
            except ValueError:
                logger.error("EDUCATION_CHANNEL_ID must be an integer (e.g. -10012345...) for the listener to work.")

            # 2. Allow admins to forward old messages to the bot to save them
            application.add_handler(
                MessageHandler(
                    filters.FORWARDED & filters.User(user_id=self.super_admin_ids), 
                    self.forward_listener
                )
            )

        # Error handler
        application.add_error_handler(self.error_handler)

        # --- MODIFIED & NEW JOBS ---
        
        # Schedule checker (every minute)
        application.job_queue.run_repeating(
            self.process_scheduled_broadcasts,
            interval=60,
            first=10
        )

        # (Existing) Leaderboard job (Sunday at 00:00 UTC)
        utc_midnight = dt_time(hour=0, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.run_leaderboards_job_v2, # <-- MODIFIED
            time=utc_midnight,
            days=(6,)  # 0=Monday, 6=Sunday
        )
        
        # NEW: Daily Tip job (Daily at 10:00 UTC)
        utc_10am = dt_time(hour=10, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.send_daily_tip,
            time=utc_10am
        )
        
        # NEW: Re-engagement job (Daily at 12:00 UTC)
        utc_12pm = dt_time(hour=12, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.re_engage_users_job,
            time=utc_12pm
        )

        # Auto-sync educational content daily (at 2 AM UTC)
        if self.edu_content_manager:
            utc_2am = dt_time(hour=2, minute=0, tzinfo=timezone.utc)
            application.job_queue.run_daily(
                self.auto_sync_education_job,
                time=utc_2am
            )

        # Daily duty assignment (midnight UTC)
        utc_midnight = dt_time(hour=0, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.assign_daily_duties_job,
            time=utc_midnight
        )

        # Duty reminder (6 PM UTC for incomplete duties)
        utc_6pm = dt_time(hour=18, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.send_duty_reminders_job,
            time=utc_6pm
        )
        # NEW: Weekly Twitter performance (Sunday 18:00 UTC)
        utc_6pm = dt_time(hour=18, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.post_weekly_performance_to_twitter,
            time=utc_6pm,
            days=(6,)  # 0=Monday, 6=Sunday
        )

        utc_end_day = dt_time(hour=23, minute=55, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.end_of_day_duty_verification_job,
            time=utc_end_day
        )
        return application

    # --- Wrapper for re-engagement job ---
    async def re_engage_users_job(self, context: ContextTypes.DEFAULT_TYPE):
        await self.engagement_tracker.re_engage_inactive_users(context)

    # --- Wrapper for promo job ---
    async def run_promo_job(self, context: ContextTypes.DEFAULT_TYPE):
        await PromotionManager.announce_promo(context, self.db)

    # --- Wrappers for new commands ---
    async def show_performance_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await PerformanceTransparency.show_verified_performance(update, context, self.db)

    async def show_referral_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        bot_username = (await context.bot.get_me()).username
        await self.referral_system.show_referral_stats(update.effective_user.id, bot_username, self.db, update)

    async def my_progress_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show user's improvement over time"""
        user_id = update.effective_user.id
        
        # Get historical data
        current_stats = self.db.get_user_signal_stats(user_id)
        current_rating = self.db.get_user_average_rating(user_id)
        
        message = (
            "📈 <b>Your Progress Report</b>\n\n"
            f"Current Rating: {current_rating:.1f}⭐\n"
            f"Signals Approved: {current_stats['approved']}/{current_stats['total']}\n"
            f"Success Rate: {current_stats['rate']:.1f}%\n\n"
        )
        
        # Add motivational element
        if current_rating >= 4.5:
            message += "🎯 <b>Outstanding!</b> You're in the elite tier!\n"
        elif current_rating >= 4.0:
            message += "💎 <b>Excellent work!</b> Keep pushing for elite status!\n"
        elif current_rating >= 3.0:
            message += "📊 <b>Good progress!</b> Focus on signal quality to level up!\n"
        else:
            message += "💪 <b>Keep learning!</b> Focus on quality signals!\n"
        
        message += "\nUse /mystats for detailed statistics"
        
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    async def suggest_broadcast_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Help admins choose optimal broadcast time"""
        
        # Analyze when users are most active
        pipeline = [
            {
                '$match': {'last_activity': {'$exists': True, '$ne': None}}
            },
            {
                '$project': {
                    'hour': {
                        '$hour': {
                            '$toDate': {'$multiply': ['$last_activity', 1000]}
                        }
                    }
                }
            },
            {
                '$group': {
                    '_id': '$hour',
                    'count': {'$sum': 1}
                }
            },
            {'$sort': {'count': -1}},
            {'$limit': 3}
        ]
        
        peak_hours = list(self.db.users_collection.aggregate(pipeline))
        
        if peak_hours:
            message = (
                "📊 <b>Optimal Broadcast Times (UTC)</b>\n\n"
                "Based on user activity patterns:\n\n"
            )
            
            for i, hour_data in enumerate(peak_hours):
                hour_utc = hour_data['_id']
                user_count = hour_data['count']
                message += f"{i+1}. {hour_utc:02d}:00 UTC ({user_count} active users)\n"
            
            message += "\n💡 Schedule broadcasts during these windows for maximum reach!"
        else:
            message = "Not enough user activity data yet."
        
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
        
    async def handle_greeting(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle greetings"""
        user_id = update.effective_user.id
        # --- FORCE SUB CHECK ---
        if not await self.is_user_subscribed(user_id, context):
            await self.send_join_channel_message(user_id, context)
            return
        # -----------------------
        await update.message.reply_text("Hello! How can I assist you today?")

    async def subscribe_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start the subscription conversation"""
        user_id = update.effective_user.id

        # --- FORCE SUB CHECK ---
        if not await self.is_user_subscribed(user_id, context):
            await self.send_join_channel_message(user_id, context)
            return ConversationHandler.END
        # -----------------------
        
        # Check if already subscribed
        if self.db.is_subscriber(user_id):
            await update.message.reply_text("✅ You are already a subscriber!")
            return ConversationHandler.END

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

        # Check if CR number is already used
        if self.db.is_cr_number_used(cr_number):
            await update.message.reply_text(
                "❌ This CR number has already been used for verification. Each CR number can only be used once.\n\n"
                "If you believe this is an error, please contact an admin."
            )
            return ConversationHandler.END

        # Check if CR number is in the valid list
        if cr_number in self.cr_numbers:
            # Mark as used
            self.db.mark_cr_number_as_used(cr_number, update.effective_user.id)

            self.engagement_tracker.update_engagement(update.effective_user.id, 'vip_subscribed')
            
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
            # A simple regex to find numbers with a dollar sign or just numbers
            matches = re.findall(r'\$?(\d[\d,]*\.\d{2})', text)
            
            if matches:
                # Clean comma from number
                balance_str = matches[0].replace(',', '')
                balance = float(balance_str)
                
                if balance >= 50:
                    user_id = update.effective_user.id
                    self.db.add_subscriber(user_id)
                    await update.message.reply_text(
                        "✅ Thank you! You have been added to the subscribers list."
                    )
                    return ConversationHandler.END
                else:
                    await update.message.reply_text(
                        f"The detected balance (${balance}) is less than $50. Please fund your account and try again."
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
        user_id = update.effective_user.id
        
        user_info = (
            f"New Currencies VIP Request:\n\n"
            f"Broker: {context.user_data['broker']}\n"
            f"Account Name: {context.user_data['account_name']}\n"
            f"Account Number: {context.user_data['account_number']}\n"
            f"Telegram ID: {context.user_data['telegram_id']}\n"
            f"User ID: {user_id}"
        )

        keyboard = [
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"vip_approve_{user_id}"),
                InlineKeyboardButton("❌ Decline", callback_data=f"vip_decline_{user_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        for admin_id in self.super_admin_ids:
            try:
                await context.bot.send_message(
                    chat_id=admin_id, 
                    text=user_info,
                    reply_markup=reply_markup
                )
            except Exception as e:
                logger.error(f"Failed to notify super admin {admin_id}: {e}")

        await update.message.reply_text(
            "Thank you! Your details have been sent to the admins for approval. You will be notified once it's reviewed."
        )
        return ConversationHandler.END

    async def handle_vip_request_review(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the approve/decline decision from an admin."""
        query = update.callback_query
        await query.answer()

        admin_id = query.from_user.id
        if admin_id not in self.super_admin_ids:
            await query.answer("You are not authorized to perform this action.", show_alert=True)
            return

        action, user_id_str = query.data.split('_')[1:]
        user_id = int(user_id_str)

        if action == "approve":
            self.db.add_subscriber(user_id)
            self.db.log_activity(admin_id, 'vip_approved', {'user_id': user_id})
            self.admin_duty_manager.credit_duty_for_action(admin_id, 'vip_approved')

            self.engagement_tracker.update_engagement(user_id, 'vip_subscribed')
            
            await query.edit_message_text(f"{query.message.text}\n\n--- ✅ Approved by {query.from_user.first_name or admin_id} ---")
            
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="Congratulations! Your Currencies VIP request has been approved. You are now a subscriber."
                )
            except Exception as e:
                logger.error(f"Failed to notify user {user_id} of approval: {e}")
            
            return ConversationHandler.END

        elif action == "decline":
            context.user_data['user_to_decline'] = user_id
            context.user_data['admin_name'] = query.from_user.first_name or admin_id
            context.user_data['original_message_text'] = query.message.text
            context.user_data['original_message_id'] = query.message.message_id
            await query.edit_message_text("Please enter the reason for declining this request.")
            return WAITING_DECLINE_REASON

    async def receive_decline_reason(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receives the decline reason from the admin and notifies the user."""
        admin_id = update.effective_user.id
        reason = update.message.text
        user_id_to_decline = context.user_data.get('user_to_decline')
        admin_name = context.user_data.get('admin_name', admin_id)
        original_message_text = context.user_data.get('original_message_text', "VIP Request")
        original_message_id = context.user_data.get('original_message_id')


        if not user_id_to_decline:
            await update.message.reply_text("Error: Could not find the user to decline. Please try again.")
            return ConversationHandler.END

        self.db.log_activity(admin_id, 'vip_declined', {'user_id': user_id_to_decline, 'reason': reason})

        try:
            await context.bot.send_message(
                chat_id=user_id_to_decline,
                text=f"We regret to inform you that your Currencies VIP request has been declined.\n\nReason: {reason}"
            )
        except Exception as e:
            logger.error(f"Failed to notify user {user_id_to_decline} of decline: {e}")
        
        await update.message.reply_text(f"The user {user_id_to_decline} has been notified of the decline.")
        
        # Restore original admin message with decline info
        if original_message_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=admin_id,
                    message_id=original_message_id,
                    text=f"{original_message_text}\n\n--- ❌ Declined by {admin_name} ---"
                )
            except Exception as e:
                 logger.error(f"Failed to edit original decline message: {e}")


        # Clean up context
        context.user_data.pop('user_to_decline', None)
        context.user_data.pop('admin_name', None)
        context.user_data.pop('original_message_text', None)
        context.user_data.pop('original_message_id', None)
        return ConversationHandler.END
        
    async def receive_schedule_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive and parse schedule time"""
        try:
            # Simple parsing for '1h 30m' or '2d'
            time_str = update.message.text.lower()
            delta = timedelta()
            
            # Regex to find time parts
            parts = re.findall(r'(\d+)\s*(d|h|m)', time_str)
            
            if parts:
                for val_str, unit in parts:
                    val = int(val_str)
                    if 'd' in unit:
                        delta += timedelta(days=val)
                    elif 'h' in unit:
                        delta += timedelta(hours=val)
                    elif 'm' in unit:
                        delta += timedelta(minutes=val)
                scheduled_time = datetime.now() + delta
            else:
                # Try parsing as absolute time
                scheduled_time = datetime.fromisoformat(time_str)

            context.user_data['scheduled_time'] = scheduled_time.timestamp()

            keyboard = [
                [InlineKeyboardButton("🔁 Once", callback_data="repeat_once")],
                [InlineKeyboardButton("🔁 Daily", callback_data="repeat_daily")],
                [InlineKeyboardButton("🔁 Weekly", callback_data="repeat_weekly")],
                [InlineKeyboardButton("🔁 Monthly", callback_data="repeat_monthly")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                f"Time set for: {scheduled_time.strftime('%Y-%m-%d %H:%M')}\n\n"
                "Set repeat interval:", 
                reply_markup=reply_markup
            )
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

    async def finalize_scheduled_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Save the scheduled broadcast to the database"""
        query = update.callback_query # 'update' is a query here
        user_id = query.from_user.id
        
        target_map = {
            'target_all': 'all',
            'target_subscribers': 'subscribers',
            'target_nonsubscribers': 'nonsubscribers',
            'target_admins': 'admins'
        }
        target = target_map.get(query.data, 'all')
        
        broadcast_message = context.user_data['broadcast_message']
        inline_buttons = context.user_data.get('inline_buttons')
        protect_content = context.user_data.get('protect_content', False)
        use_watermark = context.user_data.get('use_watermark', False)
        watermarked_image = context.user_data.get('watermarked_image')

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
            if use_watermark and watermarked_image:
                try:
                    sent_photo = await context.bot.send_photo(
                        chat_id=user_id,
                        photo=watermarked_image,
                        caption="Generating file_id for schedule..."
                    )
                    message_data['file_id'] = sent_photo.photo[-1].file_id
                    await sent_photo.delete()
                except Exception as e:
                    logger.error(f"Failed to send/delete watermarked photo for schedule: {e}")
                    message_data['file_id'] = broadcast_message.photo[-1].file_id
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

        # Schedule it
        scheduled_id = self.db.schedule_broadcast(
            message_data,
            context.user_data['scheduled_time'],
            context.user_data['repeat'],
            user_id,
            target
        )

        if scheduled_id:
            scheduled_dt = datetime.fromtimestamp(context.user_data['scheduled_time'])
            await query.edit_message_text(
                f"✅ Broadcast scheduled successfully!\n\n"
                f"Time: {scheduled_dt.strftime('%Y-%m-%d %H:%M')}\n"
                f"Target: {target.title()}\n"
                f"Repeat: {context.user_data['repeat'].title()}"
            )
        else:
            await query.edit_message_text("❌ Failed to schedule broadcast. Please try again.")

        context.user_data.clear()
        return ConversationHandler.END

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
            
            # Add user to db to fetch name
            self.db.add_user(user_id) 

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

        admin_list = "\n".join([f"• {a.get('name', a['user_id'])} ({a['role']})" for a in admins])
        await update.message.reply_text(f"👨‍💼 Admins:\n{admin_list}")

    async def view_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /logs command"""
        user_id = update.effective_user.id
        if user_id not in self.super_admin_ids:
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return
            
        logs = self.db.get_activity_logs(limit=10)
        if not logs:
            await update.message.reply_text("No activity logs found.")
            return

        log_list = "\n".join([
            f"• {datetime.fromtimestamp(log['timestamp']).strftime('%Y-%m-%d %H:%M')} "
            f"| {log['user_id']} | {log['action']} | {log.get('details', {})}"
            for log in logs
        ])
        
        message = f"📜 Last 10 Activity Logs:\n\n{log_list}"
        if len(message) > 4096:
            message = message[:4090] + "..."
            
        await update.message.reply_text(message)

    async def my_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /mystats command for both admins and users"""
        user_id = update.effective_user.id

        if self.is_admin(user_id):
            # --- Admin Statistics ---
            admin_stats = self.db.get_admin_stats(user_id)
            admin_stats_text = (
                f"📊 Your Admin Statistics\n\n"
                f"📢 Broadcasts Sent: {admin_stats.get('broadcasts', 0)}\n"
                f"📝 Templates Created: {admin_stats.get('templates', 0)}\n"
                f"⏰ Broadcasts Scheduled: {admin_stats.get('scheduled', 0)}\n"
                f"⭐ Signals Rated: {admin_stats.get('ratings', 0)}"
            )
            
            # --- Regular User Statistics (Added for Admins) ---
            signal_stats = self.db.get_user_signal_stats(user_id)
            avg_rating = self.db.get_user_average_rating(user_id)
            limit, level = self.get_user_suggestion_limit(user_id)
            rank, total_ranked = self.db.get_user_suggester_rank(user_id)

            rank_str = f"#{rank} of {total_ranked}" if rank > 0 else "Unranked"

            user_stats_text = (
                f"📈 Your Signal Stats\n\n"
                f"💡 Total Signals Suggested: {signal_stats['total']}\n"
                f"✅ Approved Signals: {signal_stats['approved']}\n"
                f"🎯 Approval Rate: {signal_stats['rate']:.1f}%\n\n"
                f"⭐ Average Rating: {avg_rating:.2f} stars\n"
                f"🏆 Current Rank: {rank_str}\n"
                f"🏅 Current Level: {level}"
            )

            # Combine both texts
            full_stats_text = f"{admin_stats_text}\n\n{'-'*30}\n\n{user_stats_text}"
            
            await update.message.reply_text(full_stats_text)
        
        else:
            # --- Regular User Statistics ---
            # --- FORCE SUB CHECK ---
            if not await self.is_user_subscribed(user_id, context):
                await self.send_join_channel_message(user_id, context)
                return
            # -----------------------

            signal_stats = self.db.get_user_signal_stats(user_id)
            avg_rating = self.db.get_user_average_rating(user_id)
            limit, level = self.get_user_suggestion_limit(user_id)
            rank, total_ranked = self.db.get_user_suggester_rank(user_id)

            rank_str = f"#{rank} of {total_ranked}" if rank > 0 else "Unranked"

            stats_text = (
                f"📈 Your Signal Stats\n\n"
                f"💡 Total Signals Suggested: {signal_stats['total']}\n"
                f"✅ Approved Signals: {signal_stats['approved']}\n"
                f"🎯 Approval Rate: {signal_stats['rate']:.1f}%\n\n"
                f"⭐ Average Rating: {avg_rating:.2f} stars\n"
                f"🏆 Current Rank: {rank_str}\n"
                f"🏅 Current Level: {level}"
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
            f"• ID: {str(b['_id'])} | "
            f"{datetime.fromtimestamp(b['scheduled_time']).strftime('%Y-%m-%d %H:%M')}"
            for b in broadcasts
        ])
        await update.message.reply_text(f"⏰ Scheduled Broadcasts:\n{broadcast_list}\n\n"
                                      f"To cancel, use /cancel_scheduled <ID>")

    async def cancel_scheduled_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /cancel_scheduled command"""
        if not self.has_permission(update.effective_user.id, Permission.SCHEDULE_BROADCASTS):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        if not context.args:
            await update.message.reply_text("❌ Please provide a broadcast ID: /cancel_scheduled <id>")
            return

        broadcast_id = context.args[0]
        
        try:
            # Validate ID format
            from bson.objectid import ObjectId
            ObjectId(broadcast_id)
        except Exception:
            await update.message.reply_text(f"❌ Invalid broadcast ID format.")
            return

        if self.db.cancel_scheduled_broadcast(broadcast_id, update.effective_user.id):
            await update.message.reply_text(f"✅ Scheduled broadcast {broadcast_id} cancelled.")
        else:
            await update.message.reply_text(f"❌ Broadcast {broadcast_id} not found or already processed.")

    # --- Forex Utility Toolkit ---

    async def news(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /news command"""
        if not self.finnhub_client:
            await update.message.reply_text("❌ The News service is currently disabled by the admin.")
            return

        try:
            # --- FORCE SUB CHECK ---
            if not await self.is_user_subscribed(update.effective_user.id, context):
                await self.send_join_channel_message(update.effective_user.id, context)
                return
            # -----------------------
            
            await update.message.reply_text("Fetching latest forex news...")
            
            forex_news = self.finnhub_client.general_news('forex', min_id=0)
            
            if not forex_news:
                await update.message.reply_text("No recent forex news found.")
                return

            message = "📰 Latest Forex News (Top 5):\n\n"
            for item in forex_news[:5]:
                message += f"▪️ <a href='{item['url']}'>{item['headline']}</a>\n"
                message += f"   <i>Source: {item['source']}</i>\n\n"

            await update.message.reply_text(message, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

        except Exception as e:
            logger.error(f"Error fetching Finnhub news: {e}")
            await update.message.reply_text("❌ An error occurred while fetching the news.")

    async def calendar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /calendar command - Temporary redirect to FXStreet"""
            
        try:
            # --- FORCE SUB CHECK ---
            if not await self.is_user_subscribed(update.effective_user.id, context):
                await self.send_join_channel_message(update.effective_user.id, context)
                return
            # -----------------------
            
            # Temporary solution: Direct users to the website
            message = (
                "🗓️ <b>Economic Calendar</b>\n\n"
                "To see the latest high-impact events, please use the official "
                "FXStreet economic calendar."
            )

            keyboard = [
                [
                    InlineKeyboardButton(
                        "View Economic Calendar", 
                        url="https://www.fxstreet.com/economic-calendar"
                    )
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                message, 
                parse_mode=ParseMode.HTML, 
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )

        except Exception as e:
            logger.error(f"Error in /calendar command: {e}")
            await update.message.reply_text("❌ An error occurred.")
    def get_pip_value(self, pair: str, lot_size: float = 1.0) -> (float, int):
        """Helper to get pip value and decimal places for a pair"""
        pair = pair.upper()
        if "JPY" in pair:
            decimals = 3
            pip_multiplier = 0.01
        elif "XAU" in pair or "GOLD" in pair: # Gold
            decimals = 2
            pip_multiplier = 0.1
        else: # Standard pairs
            decimals = 5
            pip_multiplier = 0.0001
        
        # This is a simplification. Real value depends on quote currency.
        # For a standard lot (100,000 units)
        pip_value_per_lot = pip_multiplier * 100_000
        
        # For now, let's assume quote is USD or similar
        # A 1-lot pip value is roughly $10 for XXX/USD
        # A 1-lot pip value for USD/JPY is (0.01 / JPY_PRICE) * 100,000
        
        # Let's simplify and just return decimals and pip multiplier
        return pip_multiplier, decimals

    async def pips_calculator_v2(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enhanced with context and education"""
        
        self.engagement_tracker.update_engagement(update.effective_user.id, 'command_used')
        
        if not await self.is_user_subscribed(update.effective_user.id, context):
            await self.send_join_channel_message(update.effective_user.id, context)
            return
        
        if len(context.args) != 3:
            # Helpful error message
            example = (
                "🧮 <b>Pip Calculator</b>\n\n"
                
                "<b>Usage:</b>\n"
                "<code>/pips [PAIR] [ENTRY] [EXIT]</code>\n\n"
                
                "<b>Example:</b>\n"
                "<code>/pips EURUSD 1.0850 1.0900</code>\n\n"
                
                "💡 This calculates profit/loss in pips.\n\n"
                
                "Related tools:\n"
                "/positionsize - Calculate lot size based on risk"
            )
            await update.message.reply_text(example, parse_mode=ParseMode.HTML)
            return

        try:
            pair = context.args[0].upper()
            entry = float(context.args[1])
            exit_price = float(context.args[2])
            
            pip_multiplier, decimals = self.get_pip_value(pair)
            pips = (exit_price - entry) / pip_multiplier
            
            direction = "Profit 📈" if pips > 0 else "Loss 📉"
            color = "🟢" if pips > 0 else "🔴"
            
            # Simplified value calculation
            value_per_pip_lot = 10
            if "JPY" in pair:
                 value_per_pip_lot = 1000 # This is complex, simplify
            
            estimated_value_0_1 = abs(pips) * (value_per_pip_lot * 0.1)
            
            result = (
                f"{color} <b>Pip Calculation Result</b>\n\n"
                f"<b>Pair:</b> {pair}\n"
                f"<b>Entry:</b> {entry}\n"
                f"<b>Exit:</b> {exit_price}\n\n"
                
                f"<b>Result:</b> {pips:.1f} pips ({direction})\n"
                f"<b>Est. Value (0.1 lots):</b> ~${estimated_value_0_1:.2f}\n\n"
                
                "💡 Calculate position size: /positionsize"
            )
            
            await update.message.reply_text(result, parse_mode=ParseMode.HTML)
            
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid numbers. Use format: /pips EURUSD 1.0850 1.0900"
            )

    def get_estimated_pip_value(self, pair: str) -> (float, str):
        """
        Returns (pip_value_per_lot, description)
        Estimates pip value for 1.0 standard lot in USD.
        """
        pair = pair.upper().strip()
        
        # --- Deriv / Synthetics (Standard 1.0 lot = $1 per point usually) ---
        # Note: Deriv specs vary, but 1 lot usually equals 1 unit of the index
        if any(x in pair for x in ['VOLATILITY', 'BOOM', 'CRASH', 'STEP', 'JUMP', 'V75', 'V100', 'V25']):
            # Most Deriv volatility indices: 1 Lot = $1 per 1.0 point move
            return 1.0, "Deriv (assuming $1/point)"
            
        # --- Commodities / Metals ---
        if 'XAU' in pair or 'GOLD' in pair:
            # Standard: 100oz contract. 1 pip (0.01) = $10 USD
            return 10.0, "Gold Standard ($10/pip)"
        if 'XAG' in pair or 'SILVER' in pair:
            # Standard: 5000oz contract. 1 pip (0.01) = $50 USD
            return 50.0, "Silver Standard ($50/pip)"
        if 'BTC' in pair:
            # Standard: 1 coin. 1 pip (0.01) = $0.01? No, usually $1 move = $1 USD
            return 1.0, "Crypto ($1/1.0 move)"
        if 'US30' in pair or 'DJ30' in pair:
             # Standard: 1 Lot = $1 per point (variable by broker, often $5 or $1)
             return 1.0, "Index (assuming $1/point)"

        # --- Forex Majors (USD is Counter) ---
        # EURUSD, GBPUSD, AUDUSD, NZDUSD
        if pair.endswith('USD'):
            return 10.0, "Standard ($10/pip)"
            
        # --- Forex Crosses (Approximations based on current rates) ---
        # Accuracy Note: Without a live price feed, we use static averages.
        if 'JPY' in pair:
            # 1000 units / USDJPY rate (approx 150) = $6.66
            return 6.66, "JPY Pair (~$6.66/pip)"
        if 'CAD' in pair:
            # 10 USD / USDCAD rate (approx 1.35) = $7.40
            return 7.40, "CAD Pair (~$7.40/pip)"
        if 'CHF' in pair:
            # 10 USD / USDCHF rate (approx 0.88) = $11.36
            return 11.30, "CHF Pair (~$11.30/pip)"
        if 'GBP' in pair: # EURGBP
            # 10 * GBPUSD rate (approx 1.27) = $12.70
            return 12.70, "GBP Cross (~$12.70/pip)"
        if 'EUR' in pair: # EURAUD etc
             # 10 * EURUSD rate (approx 1.08) = $10.80
            return 10.80, "EUR Cross (~$10.80/pip)"
            
        # Fallback
        return 10.0, "Standard (Approx)"

    async def position_size_calculator(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Professional Position Size Calculator
        Usage: /positionsize [PAIR] [RISK_USD] [SL_PIPS]
        """
        self.engagement_tracker.update_engagement(update.effective_user.id, 'command_used')

        # Check subscription
        if not await self.is_user_subscribed(update.effective_user.id, context):
            await self.send_join_channel_message(update.effective_user.id, context)
            return

        if len(context.args) != 3:
            example_text = (
                "Usage: /positionsize [pair] [risk_usd] [stop_loss_pips]\n"
                "Example: <code>/positionsize EURUSD 100 20</code>\n\n"
                "<i>Supports Forex, Gold, and Deriv (V75, Boom, Crash)</i>"
            )
            await update.message.reply_text(example_text, parse_mode=ParseMode.HTML)
            return

        try:
            # 1. Parse Arguments
            pair = context.args[0].upper()
            risk_usd = float(context.args[1])
            sl_pips = float(context.args[2])
            
            if risk_usd <= 0 or sl_pips <= 0:
                await update.message.reply_text("❌ Risk and SL must be positive numbers.")
                return

            # 2. Get Pip Value Logic
            pip_value_per_lot, description = self.get_estimated_pip_value(pair)
            
            # 3. Calculate Lot Size
            # Formula: Risk / (StopLoss * PipValue)
            if pip_value_per_lot > 0:
                raw_lots = risk_usd / (sl_pips * pip_value_per_lot)
            else:
                raw_lots = 0

            # 4. Rounding Logic
            if "V75" in pair or "VOLATILITY" in pair:
                recommended_lots = round(raw_lots, 3) 
                if recommended_lots < 0.001: recommended_lots = 0.001
            else:
                recommended_lots = round(raw_lots, 2)
                if recommended_lots < 0.01: recommended_lots = 0.01

            # 5. Format Output (FIXED: Changed <small> to <i>)
            message = (
                "📐 <b>Position Size Calculator</b>\n\n"
                f"Risk: ${risk_usd:,.2f}\n"
                f"Stop Loss: {sl_pips} pips/points\n"
                f"Pair: {pair} <i>({description})</i>\n\n"
                f"Recommended Lot Size: <b>{recommended_lots} lots</b>"
            )

            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        except ValueError:
            await update.message.reply_text("❌ Invalid format. Please use numbers for Risk and SL.")
        except Exception as e:
            logger.error(f"Error in position size: {e}")
            await update.message.reply_text("❌ Calculation error.")

    async def send_daily_tip(self, context: ContextTypes.DEFAULT_TYPE):
        """Send random educational content from database channel"""
        
        if not self.edu_content_manager:
            logger.warning("Educational content manager not initialized. Skipping daily tip.")
            return
        
        # Get all users who haven't opted out
        all_users = self.db.get_all_users()
        target_users = {
            user_id for user_id in all_users 
            if self.notification_manager.should_notify(user_id, 'tips')
        }
        
        if not target_users:
            logger.info("No users to send educational content to")
            return
        
        # Broadcast random educational content
        success, failed = await self.edu_content_manager.broadcast_random_content(
            context, 
            target_users
        )
        
        logger.info(f"Daily educational content sent: {success} success, {failed} failed")
        content = await self.edu_content_manager.get_random_content()
        if content:
            await self.twitter.post_daily_tip(content)

    async def post_weekly_performance_to_twitter(self, context: ContextTypes.DEFAULT_TYPE):
        """Job: Weekly transparency post on Twitter"""
        thirty_days_ago = time.time() - (30 * 86400)
        
        pipeline = [
            {
                '$match': {
                    'status': 'approved',
                    'reviewed_at': {'$gte': thirty_days_ago},
                    'rating': {'$exists': True}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'total_signals': {'$sum': 1},
                    'avg_rating': {'$avg': '$rating'},
                    'excellent_signals': {
                        '$sum': {'$cond': [{'$gte': ['$rating', 4]}, 1, 0]}
                    }
                }
            }
        ]
        
        stats_list = list(self.db.signal_suggestions_collection.aggregate(pipeline))
        
        if stats_list:
            await self.twitter.post_performance_update(stats_list[0])

    async def settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Let users control their experience"""
        
        self.engagement_tracker.update_engagement(update.effective_user.id, 'command_used')
        user_id = update.effective_user.id
        prefs = self.notification_manager.get_notification_preferences(user_id)
        
        user_doc = self.db.users_collection.find_one({'user_id': user_id}) or {}
        leaderboard_public = user_doc.get('leaderboard_public', True)

        # Build keyboard
        keyboard = []
        for key, desc in [
            ('tips', 'Daily Tips'),
            ('leaderboards', 'Leaderboards'),
            ('promo', 'Promotions'),
            ('signals', 'Signal Suggestions'),
            ('broadcasts', 'Admin Signals & Announcements')
        ]:
            if key in prefs:
                status = '✅ ON' if prefs[key] else '❌ OFF'
                keyboard.append([
                    InlineKeyboardButton(f"{desc}: {status}", callback_data=f"toggle_notify_{key}")
                ])

        keyboard.append([InlineKeyboardButton(
            f"Show in Leaderboard: {'✅ YES' if leaderboard_public else '❌ NO'}",
            callback_data="toggle_leaderboard"
        )])
        keyboard.append([InlineKeyboardButton("Done", callback_data="close_settings")])
        
        message = (
            "⚙️ <b>Your Settings</b>\n\n"
            "Manage your notifications and privacy:\n"
        )
        
        await update.message.reply_text(
            message,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def handle_settings_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle toggling settings"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        if query.data == "close_settings":
            try:
                await query.edit_message_text("✅ Settings saved!")
            except Exception as e:
                logger.info(f"Settings close message already edited: {e}")
            return

        try:
            toggle_type, key = query.data.split('_', 1)

            if toggle_type == "toggle":
                if key == "leaderboard":
                    # Toggle privacy
                    user_doc = self.db.users_collection.find_one({'user_id': user_id}) or {}
                    new_status = not user_doc.get('leaderboard_public', True)
                    self.db.users_collection.update_one(
                        {'user_id': user_id},
                        {'$set': {'leaderboard_public': new_status}}
                    )
                elif key.startswith("notify_"):
                    # This is the fix: extract the actual key (e.g., 'tips')
                    actual_key = key.split('_', 1)[1]
                    
                    prefs = self.notification_manager.get_notification_preferences(user_id)
                    new_status = not prefs.get(actual_key, True)
                    self.db.users_collection.update_one(
                        {'user_id': user_id},
                        {'$set': {f'notifications.{actual_key}': new_status}}
                    )
                else:
                    logger.warning(f"Unknown settings toggle key: {key}")

            # Re-fetch and re-draw the menu
            prefs = self.notification_manager.get_notification_preferences(user_id)
            user_doc = self.db.users_collection.find_one({'user_id': user_id}) or {}
            leaderboard_public = user_doc.get('leaderboard_public', True)
            
            keyboard = []
            for key_loop, desc in [
                ('tips', 'Daily Tips'),
                ('leaderboards', 'Leaderboards'),
                ('promo', 'Promotions'),
                ('signals', 'Signal Suggestions'),
                ('broadcasts', 'Admin Signals & Announcements')
            ]:
                if key_loop in prefs:
                    status = '✅ ON' if prefs[key_loop] else '❌ OFF'
                    keyboard.append([
                        InlineKeyboardButton(f"{desc}: {status}", callback_data=f"toggle_notify_{key_loop}")
                    ])

            keyboard.append([InlineKeyboardButton(
                f"Show in Leaderboard: {'✅ YES' if leaderboard_public else '❌ NO'}",
                callback_data="toggle_leaderboard"
            )])
            keyboard.append([InlineKeyboardButton("Done", callback_data="close_settings")])
            
            message = (
                "⚙️ <b>Your Settings</b>\n\n"
                "Manage your notifications and privacy:\n"
            )
            
            await query.edit_message_text(
                message,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        except Exception as e:
            # Catch any other errors and report them
            logger.error(f"Error in handle_settings_callback: {e}")
            try:
                await query.answer("An error occurred. Please try again.", show_alert=True)
            except:
                pass

def main():
    """Main function"""
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    ADMIN_IDS = os.getenv('ADMIN_IDS', '').split(',')
    MONGODB_URI = os.getenv('MONGODB_URI')
    FORCE_SUB_CHANNEL = os.getenv('FORCE_SUB_CHANNEL') # <-- ADD THIS
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN required")
    if not ADMIN_IDS or ADMIN_IDS == ['']:
        raise ValueError("ADMIN_IDS required")

    if not MONGODB_URI:
        raise ValueError("MONGODB_URI required")

    try:
        # --- FIX: This line should be indented inside main() ---
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

    if FORCE_SUB_CHANNEL:
        logger.info(f"Force-sub feature is ENABLED for channel: {FORCE_SUB_CHANNEL}")
    else:
        logger.info("Force-sub feature is DISABLED (FORCE_SUB_CHANNEL not set)")

    bot.run_health_server(port)

    time.sleep(2)

    logger.info("Starting Telegram bot...")
    try:
        application.run_polling()
    finally:
        mongo_handler.close()


if __name__ == '__main__':
    main()
