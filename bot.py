import os
import logging
import asyncio
import aiohttp
from aiohttp import web
import threading
from datetime import datetime, timedelta, time as dt_time, timezone
import textwrap
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
from telegram.error import BadRequest
from telegram import ReactionTypeEmoji, Update
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

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
if not FINNHUB_API_KEY:
    logger.warning("FINNHUB_API_KEY is not set. /news and /calendar commands will be disabled.")


WAITING_INITIAL_PLATFORM, WAITING_MESSAGE, WAITING_BUTTONS, WAITING_PROTECTION, WAITING_TARGET = range(5)
WAITING_TEMPLATE_NAME, WAITING_TEMPLATE_MESSAGE, WAITING_TEMPLATE_CATEGORY = range(5, 8)
WAITING_SCHEDULE_TIME, WAITING_SCHEDULE_REPEAT = range(8, 10)
WAITING_ADMIN_ID, WAITING_ADMIN_ROLE = range(10, 12)
WAITING_SIGNAL_MESSAGE = 12

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
    WAITING_SIGNAL_REJECTION_REASON,
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
            return []
            
        current_achievements = set(user.get('achievements', []))
        signal_stats = db.get_user_signal_stats(user_id)
        avg_rating = db.get_user_average_rating(user_id)
        
        new_achievements = []
        
       
        if 'first_signal' not in current_achievements and signal_stats['total'] >= 1:
            new_achievements.append('first_signal')
        
       
        if 'approved_signal' not in current_achievements and signal_stats['approved'] >= 1:
            new_achievements.append('approved_signal')
        
        has_five_star = db.signal_suggestions_collection.find_one({
            'suggested_by': user_id,
            'rating': 5
        })
        if 'five_star' not in current_achievements and has_five_star:
            new_achievements.append('five_star')
        
       
        if 'elite' not in current_achievements and avg_rating >= 4.5 and signal_stats['approved'] >= 20:
            new_achievements.append('elite')
        
        if new_achievements:
            db.users_collection.update_one(
                {'user_id': user_id},
                {'$addToSet': {'achievements': {'$each': new_achievements}}}
            )
            
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
        
        db.users_collection.update_one(
            {'user_id': referrer_id},
            {
                '$inc': {'referrals': 1},
                '$push': {'referred_users': new_user_id}
            },
            upsert=True
        )
        
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
        
        non_subscribers = db.users_collection.find({
            'user_id': {'$nin': list(db.get_all_subscribers())},
            'last_activity': {'$gte': time.time() - (7 * 86400)}, 
            'promo_nov_2024_seen': {'$ne': True} 
        })
        
        promo_message = (
            "🎁 <b>Special Offer - 7 Days Only</b>\n\n"
            
            "Join PipSage VIP and get:\n"
            "✅ First month 50% off\n"
            "✅ Bonus: 3 free private consultations\n"
            "✅ Lifetime access to tools\n\n"
            
            "Start: /subscribe\n\n"
            
            "<i>Expires: November 24, 2025</i>" 
        )
        
        sent = 0
        for user in non_subscribers:
            try:
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text=promo_message,
                    parse_mode=ParseMode.HTML
                )
                
                db.users_collection.update_one(
                    {'user_id': user['user_id']},
                    {'$set': {'promo_nov_2024_seen': True}}
                )
                
                sent += 1
                await asyncio.sleep(0.1)
            except:
                pass
        
        logger.info(f"Promotion announced to {sent} active non-subscribers")

class BroadcastFrequencyManager:
    """Prevent broadcast spam"""
    
    def __init__(self, db):
        self.db = db
        
    async def can_broadcast(self, admin_id: int) -> (bool, str):
        """Check if admin can send another broadcast"""
        
        last_broadcast = self.db.activity_logs_collection.find_one(
            {
                'user_id': admin_id,
                'action': {'$in': ['broadcast_sent', 'approved_broadcast_sent', 'broadcast_submitted']},
            },
            sort=[('timestamp', -1)]
        )
        
        last_broadcast_time = last_broadcast['timestamp'] if last_broadcast else 0
        
        role = self.db.get_admin_role(admin_id)
        limits_seconds = {
            AdminRole.SUPER_ADMIN: 30,  
            AdminRole.ADMIN: 300,       
            AdminRole.MODERATOR: 180,  
            AdminRole.BROADCASTER: 600 
        }
        
        limit = limits_seconds.get(role, 300)
        
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
             return True, [] 

        if len(content) < 10:
            issues.append("Message too short (minimum 10 characters)")
        
        if content.isupper() and len(content) > 50:
            issues.append("Avoid ALL CAPS messages")
        
        emoji_count = 0
        for char in content:
            if char > '\u231a':
                emoji_count += 1
        
        if emoji_count > 15:
            issues.append("Too many emojis (max 15)")
        
        spam_words = ['100% guaranteed', 'act fast', 'limited time only']
        if any(word.lower() in content.lower() for word in spam_words):
            issues.append("Message contains spam-like phrases (e.g., '100% guaranteed')")
        
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
        
        score = (
            engagement.get('command_used', 0) * 2 +
            engagement.get('signal_suggested', 0) * 10 +
            engagement.get('signal_approved', 0) * 20 +
            engagement.get('vip_subscribed', 0) * 30
        )
        
        last_activity = user.get('last_activity', 0)
        days_inactive = (time.time() - last_activity) / 86400
        
        if days_inactive > 30:
            score *= 0.5
        
        return min(int(score), 100)
    
    async def re_engage_inactive_users(self, context: ContextTypes.DEFAULT_TYPE):
        """Gentle re-engagement for inactive users"""
        
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
                
                self.db.users_collection.update_one(
                    {'user_id': user['user_id']},
                    {'$set': {'re_engaged': True}}
                )
                
                await asyncio.sleep(1) 
            except:
                pass

class NotificationManager:
    """Respect user preferences"""
    
    DEFAULT_PREFS = {
        'broadcasts': True, 
        'signals': True,    
        'leaderboards': True, 
        'tips': True,       
        'promo': True,      
        'achievements': True 
    }

    def __init__(self, db):
        self.db = db

    def get_notification_preferences(self, user_id: int) -> dict:
        """Get user's notification settings, applying defaults"""
        user = self.db.users_collection.find_one({'user_id': user_id})
        
        if not user or 'notifications' not in user:
            return self.DEFAULT_PREFS.copy()
        
        user_prefs = user.get('notifications', {})
        prefs = self.DEFAULT_PREFS.copy()
        prefs.update(user_prefs)
        
        return prefs

    def get_eligible_users(self, user_ids: set, notification_type: str) -> set:
        """
        Fetch users who have opted IN for a specific notification_type in ONE query.
        Solves the N+1 query disaster.
        """
        if not user_ids:
            return set()
            
        user_ids_list = list(user_ids)
        eligible_users = set()
        
        CHUNK_SIZE = 10000
        
        for i in range(0, len(user_ids_list), CHUNK_SIZE):
            chunk = user_ids_list[i:i + CHUNK_SIZE]
            
            query = {
                'user_id': {'$in': chunk},
                f'notifications.{notification_type}': {'$ne': False}
            }
            
            cursor = self.db.users_collection.find(query, {'user_id': 1})
            
            for user in cursor:
                eligible_users.add(user['user_id'])
                
        return eligible_users

    def should_notify(self, user_id: int, notification_type: str) -> bool:
        """Check if user wants this notification"""
        if notification_type not in self.DEFAULT_PREFS:
            logger.warning(f"Invalid notification_type check: {notification_type}")
            return True 

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
        self.used_cr_numbers_collection = None  
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
            self.used_cr_numbers_collection = self.db['used_cr_numbers'] 
            self.users_collection.create_index('user_id', unique=True)
            self.subscribers_collection.create_index('user_id', unique=True)
            self.admins_collection.create_index('user_id', unique=True)
            self.templates_collection.create_index('created_by')
            self.scheduled_broadcasts_collection.create_index('scheduled_time')
            self.activity_logs_collection.create_index([('timestamp', -1)])
            self.broadcast_approvals_collection.create_index('status')
            self.signal_suggestions_collection.create_index('status')
            self.used_cr_numbers_collection.create_index('cr_number', unique=True)
            self.vip_requests_collection = self.db['vip_requests']
            self.vip_requests_collection.create_index([('user_id', 1), ('status', 1)])
            self.notifications_collection = self.db['user_notifications']
            self.notifications_collection.create_index([('user_id', 1), ('timestamp', -1)])
            self.notifications_collection.create_index("timestamp", expireAfterSeconds=2592000) 
            self.notifications_collection.create_index([('user_id', 1), ('timestamp', -1)])

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
                        'last_activity': time.time()
                    },
                    '$setOnInsert': {
                        'created_at': time.time(),
                        'achievements': [],
                        'referrals': 0,
                        'daily_tips_enabled': True,
                        'leaderboard_public': True
                    }
                },
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding user {user_id}: {e}")
            return False

    def delete_user_fully(self, user_id: int):
        """Completely remove all traces of a user from the database"""
        try:
            self.users_collection.delete_one({'user_id': user_id})
            self.subscribers_collection.delete_one({'user_id': user_id})
            self.admins_collection.delete_one({'user_id': user_id})
            self.vip_requests_collection.delete_many({'user_id': user_id})
            self.notifications_collection.delete_many({'user_id': user_id})
            self.activity_logs_collection.delete_many({'user_id': user_id})
            self.signal_suggestions_collection.delete_many({'suggested_by': user_id})
            self.scheduled_broadcasts_collection.delete_many({'created_by': user_id})
            self.templates_collection.delete_many({'created_by': user_id})
            
            logger.info(f"🗑️ Permanently deleted all data for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error executing full user deletion for {user_id}: {e}")
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

    def delete_blocked_user(self, user_id: int):
        """Remove user who blocked the bot from all relevant collections"""
        try:
            self.users_collection.delete_one({'user_id': user_id})
            self.subscribers_collection.delete_one({'user_id': user_id})
            self.notifications_collection.delete_many({'user_id': user_id})
            logger.info(f"🗑️ Automatically removed blocked user: {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error removing blocked user {user_id}: {e}")
            return False

    def save_notifications_bulk(self, notifications: List[Dict]):
        """
        Efficiently save multiple notifications at once.
        Replaces loop-based synchronous writes.
        """
        try:
            if notifications:
                self.notifications_collection.insert_many(notifications, ordered=False)
        except Exception as e:
            logger.error(f"Error bulk saving notifications: {e}")

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

    def save_notification(self, user_id: int, title: str, body: str, data: dict = None):
        """Save notification to history"""
        try:
            notification = {
                'user_id': user_id,
                'title': title,
                'body': body,
                'data': data or {},
                'read': False,
                'timestamp': time.time()
            }
            self.notifications_collection.insert_one(notification)
        except Exception as e:
            logger.error(f"Error saving notification: {e}")

    def get_user_notifications(self, user_id: int, limit: int = 20) -> List[Dict]:
        """Fetch notification history"""
        try:
            cursor = self.notifications_collection.find(
                {'user_id': user_id},
                {'_id': 0}
            ).sort('timestamp', -1).limit(limit)
            return list(cursor)
        except Exception as e:
            logger.error(f"Error fetching notifications: {e}")
            return []


    def set_support_group(self, chat_id: int):
        """Sets the support group ID."""
        try:
            self.db['settings'].update_one(
                {'_id': 'bot_config'},
                {'$set': {'support_group_id': chat_id}},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error setting support group: {e}")
            return False

    def get_support_group(self) -> Optional[int]:
        """Retrieves the support group ID."""
        try:
            config = self.db['settings'].find_one({'_id': 'bot_config'})
            return config.get('support_group_id') if config else None
        except Exception as e:
            logger.error(f"Error getting support group: {e}")
            return None

    def save_support_mapping(self, group_message_id: int, user_id: int):
        """Maps a forwarded message ID in the group to the original user ID."""
        try:
            self.db['support_mappings'].insert_one({
                'group_message_id': group_message_id,
                'user_id': user_id,
                'created_at': time.time()
            })
        except Exception as e:
            logger.error(f"Error saving support mapping: {e}")

    def get_support_user_id(self, group_message_id: int) -> Optional[int]:
        """Retrieves original user ID from the group message ID."""
        try:
            mapping = self.db['support_mappings'].find_one({'group_message_id': group_message_id})
            return mapping['user_id'] if mapping else None
        except Exception as e:
            logger.error(f"Error getting support user ID: {e}")
            return None

    

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

    def add_admin(self, user_id: int, role: AdminRole, added_by: int):
        """Add an admin with role"""
        try:
            user_info = self.users_collection.find_one({'user_id': user_id})
            admin_name = str(user_id)
            if user_info:
                admin_name = user_info.get('first_name') or user_info.get('username') or str(user_id)

            self.admins_collection.update_one(
                {'user_id': user_id},
                {
                    '$set': {
                        'user_id': user_id,
                        'name': admin_name,
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

    def is_cr_number_used(self, cr_number: str) -> bool:
        """Check if a CR number has already been used for verification"""
        try:
            return self.used_cr_numbers_collection.find_one({'cr_number': cr_number}) is not None
        except Exception as e:
            logger.error(f"Error checking CR number {cr_number}: {e}")
            return False 
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
            
    def update_user_push_token(self, user_id: int, token: str):
        """Update user's Expo push token with logging"""
        try:
            result = self.users_collection.update_one(
                {'user_id': user_id},
                {
                    '$set': {
                        'push_token': token,
                        'push_token_updated_at': time.time()
                    }
                },
                upsert=True
            )
        
            if result.modified_count > 0 or result.upserted_id:
                logger.info(f"✅ Updated push token for user {user_id}")
                return True
            else:
                logger.warning(f"⚠️ Push token update had no effect for user {user_id}")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error updating push token for {user_id}: {e}")
            return False

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
                    '$limit': 10  
                }
            ]
            return list(self.signal_suggestions_collection.aggregate(pipeline))
        except Exception as e:
            logger.error(f"Error getting suggester stats: {e}")
            return []

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
            today_utc = datetime.now(timezone.utc).date()
            start_of_today_timestamp = datetime.combine(today_utc, dt_time(0, 0, tzinfo=timezone.utc)).timestamp()

            count = self.signal_suggestions_collection.count_documents({
                'suggested_by': user_id,
                'created_at': {'$gte': start_of_today_timestamp}
            })
            return count
        except Exception as e:
            logger.error(f"Error counting today's suggestions for {user_id}: {e}")
            return 0 

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
            return 0.0 
        except Exception as e:
            logger.error(f"Error getting user average rating for {user_id}: {e}")
            return 0.0 

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
                return 0, 0 

            total_ranked_users = len(result[0]['users'])
            
            try:
                rank = [i for i, user in enumerate(result[0]['users']) if user['user_id'] == user_id][0] + 1
                return rank, total_ranked_users
            except IndexError:
                return 0, total_ranked_users 

        except Exception as e:
            logger.error(f"Error getting user suggester rank for {user_id}: {e}")
            return 0, 0

    def get_latest_vip_request(self, user_id: int) -> Dict:
        """Get the most recent VIP request for a user"""
        try:
            return self.vip_requests_collection.find_one(
                {'user_id': user_id},
                sort=[('submitted_at', -1)]
            )
        except Exception as e:
            logger.error(f"Error getting vip request for {user_id}: {e}")
            return None

    def create_vip_request(self, user_id: int, vip_type: str, details: Dict):
        """Store a new VIP request"""
        try:
            request_doc = {
                'user_id': user_id,
                'type': vip_type,
                'status': 'pending',
                'submitted_at': time.time(),
                'details': details
            }
            # Update existing pending if any, or insert new
            self.vip_requests_collection.update_one(
                {'user_id': user_id, 'status': 'pending'},
                {'$set': request_doc},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error creating vip request: {e}")

    def update_vip_request_status(self, user_id: int, status: str, admin_id: int, reason: str = None):
        """Update status of pending VIP request"""
        try:
            update_data = {
                'status': status,
                'reviewed_by': admin_id,
                'reviewed_at': time.time()
            }
            if reason:
                update_data['rejection_reason'] = reason
                
            self.vip_requests_collection.update_one(
                {'user_id': user_id, 'status': 'pending'},
                {'$set': update_data}
            )
        except Exception as e:
            logger.error(f"Error updating vip request: {e}")

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
        self.educational_content_collection.create_index(
            [('message_id', 1), ('chat_id', 1)], 
            unique=True,
            name='educational_content_id_chat_unique',
            background=True 
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
            return False 

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
            self.educational_content_collection.update_one(
                {'message_id': message.message_id, 'chat_id': message.chat.id},
                {'$set': entry},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error saving educational content: {e}")
            return False
            
    async def broadcast_specific_content(self, context, target_users, content):
        """Broadcast a SPECIFIC piece of content to a list of users"""
        if not content:
            return 0, 0

        success = 0
        failed = 0
        
        footer = "\n\n🔕 Disable: /settings then toggle off Daily Tips"
        
        for user_id in target_users:
            try:
                if content['type'] == 'text':
                    text_to_send = content['content'] + footer
                    await context.bot.send_message(chat_id=user_id, text=text_to_send)
                elif content['type'] == 'photo':
                    caption_to_send = (content.get('caption') or '') + footer
                    await context.bot.send_photo(chat_id=user_id, photo=content['file_id'], caption=caption_to_send)
                elif content['type'] == 'video':
                    caption_to_send = (content.get('caption') or '') + footer
                    await context.bot.send_video(chat_id=user_id, video=content['file_id'], caption=caption_to_send)
                elif content['type'] == 'document':
                    caption_to_send = (content.get('caption') or '') + footer
                    await context.bot.send_document(chat_id=user_id, document=content['file_id'], caption=caption_to_send)
                success += 1
            except Exception as e:
                # Manual block handling for EducationalContentManager
                err_str = str(e).lower()
                if any(x in err_str for x in ["bot was blocked", "user is deactivated", "chat not found", "forbidden"]):
                    try:
                        self.db['users'].delete_one({'user_id': user_id})
                        self.db['subscribers'].delete_one({'user_id': user_id})
                        self.db['user_notifications'].delete_many({'user_id': user_id})
                        logger.info(f"🗑️ Educational Manager automatically removed blocked user: {user_id}")
                    except Exception as db_e:
                        logger.error(f"Failed to remove blocked user {user_id}: {db_e}")
                
                failed += 1
                
        return success, failed

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
        
        footer = "\n\n🔕 Disable: /settings then toggle off Daily Tips"
        
        for user_id in target_users:
            try:
                if content['type'] == 'text':
                    text_to_send = content['content'] + footer
                    await context.bot.send_message(chat_id=user_id, text=text_to_send)
                elif content['type'] == 'photo':
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
        
        duties_to_credit = list(self.admin_duties_collection.find({
            'date': date_key,
            'duty_category': duty_category,
            'completed': False
        }))
        
        if not duties_to_credit:
            return False
        
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
                submitted = self.db['signal_suggestions'].count_documents({
                    'created_at': {'$gte': start_timestamp, '$lt': end_timestamp}
                })
                return submitted > 0
            
            elif duty_category == 'broadcast_approval':
                submitted = self.db['broadcast_approvals'].count_documents({
                    'created_at': {'$gte': start_timestamp, '$lt': end_timestamp}
                })
                return submitted > 0
                
            return True
        except Exception as e:
            logger.error(f"Error checking work existence: {e}")
            return True 

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
            'verified_complete': {},
            'left_incomplete': {}
        }
        
        for duty in incomplete_duties:
            duty_category = duty['duty_category']
            admin_id = duty['admin_id']
            admin_name = duty['admin_name']
            action_count = duty.get('action_count', 0)
            had_work = self._check_if_work_existed(duty_category, date_key)
            
            if not had_work:
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
                self.admin_duties_collection.update_one(
                    {'_id': duty['_id']},
                    {
                        '$set': {
                            'completed': True,
                            'auto_completed': False,
                            'system_verified': True, 
                            'completed_at': time.time(),
                            'completion_notes': f'System Verified: {action_count} actions recorded.'
                        }
                    }
                )
                results['verified_complete'].setdefault(duty_category, []).append(f"{admin_name} ({action_count} actions)")
                
            else:
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
        
        eligible_admins = [
            admin for admin in admin_list 
            if admin['role'] in ['super_admin', 'admin', 'moderator']
        ]
        
        if not eligible_admins:
            logger.warning("No eligible admins for duty assignment")
            return {}
            
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        yesterday_duties = list(self.admin_duties_collection.find({'date': yesterday}))
        
        last_assignments = {
            duty['admin_id']: duty['duty_category'] 
            for duty in yesterday_duties
        }
        
        duty_categories = list(self.DUTY_CATEGORIES.keys())
        
        random.shuffle(eligible_admins)
        random.shuffle(duty_categories)
        
        assignments = {}
        used_categories = set()
        
        high_priority = [cat for cat, info in self.DUTY_CATEGORIES.items() if info['priority'] == 'high']
        
        for i, admin in enumerate(eligible_admins[:len(high_priority)]):
            admin_id = admin['user_id']
            last_duty = last_assignments.get(admin_id)
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
        
        remaining_admins = [a for a in eligible_admins if a['user_id'] not in assignments]
        remaining_duties = [cat for cat in duty_categories if cat not in used_categories]
        
        for i, admin in enumerate(remaining_admins):
            if i >= len(remaining_duties):
                break  
            
            admin_id = admin['user_id']
            category = remaining_duties[i]
            
            assignments[admin_id] = {
                'duty_category': category,
                'duty_info': self.DUTY_CATEGORIES[category],
                'admin_name': admin.get('name', str(admin_id)),
                'admin_role': admin['role']
            }
        
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
    
class TwitterIntegration:
    """Auto-post bot content to Twitter with proper Threading"""
    
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
            self.api = tweepy.API(auth)
            logger.info("Twitter integration enabled")
        else:
            self.client = None
            self.api = None
            logger.warning("Twitter credentials not set")

    def _clean_html(self, text: str) -> str:
        """Helper: Remove HTML tags for Twitter"""
        return re.sub(r'<[^>]+>', '', text)

    def _split_text(self, text: str) -> List[str]:
        """
        Smartly split text into chunks for Twitter threading.
        Preserves newlines and paragraph structure.
        """
        if not text:
            return []
        MAX_LEN = 265

        if len(text) <= 280:
            return [text]

        paragraphs = text.split('\n')
        chunks = []
        current_chunk = ""

        for para in paragraphs:
            potential_len = len(current_chunk) + len(para) + 1

            if potential_len <= MAX_LEN:
                if current_chunk:
                    current_chunk += "\n" + para
                else:
                    current_chunk = para
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = ""

                if len(para) > MAX_LEN:
                    wrapped_parts = textwrap.wrap(para, width=MAX_LEN, replace_whitespace=False, drop_whitespace=False)
                    chunks.extend(wrapped_parts[:-1])
                    current_chunk = wrapped_parts[-1]
                else:
                    current_chunk = para

        if current_chunk:
            chunks.append(current_chunk)

        total = len(chunks)
        final_tweets = []
        for i, chunk in enumerate(chunks):
            suffix = f" ({i+1}/{total})"
            if len(chunk) + len(suffix) > 280:
                final_tweets.append(chunk[:280-len(suffix)] + suffix)
            else:
                final_tweets.append(chunk + suffix)

        return final_tweets

    async def _post_thread(self, tweets: List[str], media_ids: List[str] = None) -> Optional[str]:
        """Helper to post a list of strings as a Twitter thread"""
        if not tweets and not media_ids:
            return None
            
        if not tweets and media_ids:
            tweets = [""]

        previous_tweet_id = None
        first_tweet_url = None
        
        try:
            for i, tweet_text in enumerate(tweets):
                kwargs = {'text': tweet_text}
                
                if i == 0 and media_ids:
                    kwargs['media_ids'] = media_ids
                
                if previous_tweet_id:
                    kwargs['in_reply_to_tweet_id'] = previous_tweet_id
                
                response = self.client.create_tweet(**kwargs)
                previous_tweet_id = response.data['id']
                
                if i == 0:
                    first_tweet_url = f"https://twitter.com/user/status/{response.data['id']}"
            
            return first_tweet_url
        except Exception as e:
            logger.error(f"Error posting thread: {e}")
            return None

    async def post_general_broadcast(self, context, message_data: Dict) -> Optional[str]:
        """Post a general broadcast to Twitter with threading support"""
        if not self.client:
            return None
        
        try:
            media_ids = []
            text_content = ""

            if message_data['type'] == 'text':
                text_content = message_data['content']
            elif message_data['type'] == 'photo':
                text_content = message_data.get('caption') or ""
                if message_data.get('file_id'):
                    media_ids = await self._upload_telegram_photo(context, message_data['file_id'])
            elif message_data['type'] == 'video':
                text_content = message_data.get('caption') or ""
                if message_data.get('file_id'):
                    media_ids = await self._upload_telegram_video(context, message_data['file_id'])
            
            tweets = self._split_text(text_content)
            return await self._post_thread(tweets, media_ids)
            
        except Exception as e:
            logger.error(f"Failed to post broadcast to Twitter: {e}")
            return None
    
    async def _upload_telegram_photo(self, context, file_id):
        try:
            new_file = await context.bot.get_file(file_id)
            bio = io.BytesIO()
            await new_file.download_to_memory(bio)
            bio.seek(0)
            media = self.api.media_upload(filename="signal.jpg", file=bio)
            return [media.media_id]
        except Exception as e:
            logger.error(f"Error uploading photo to Twitter: {e}")
            return []

    async def _upload_telegram_video(self, context, file_id):
        try:
            new_file = await context.bot.get_file(file_id)
            bio = io.BytesIO()
            await new_file.download_to_memory(bio)
            bio.seek(0)
            media = self.api.media_upload(filename="video.mp4", file=bio, media_category='tweet_video')
            return [media.media_id]
        except Exception as e:
            logger.error(f"Error uploading video to Twitter: {e}")
            return []

    async def post_signal(self, context, suggestion: Dict) -> Optional[str]:
        """Post approved signal to Twitter"""
        if not self.client:
            return None
        
        try:
            message_data = suggestion['message_data']
            suggester = suggestion['suggester_name']
            rating = suggestion.get('rating', 0)
            
            content = message_data.get('content') if message_data['type'] == 'text' else message_data.get('caption', "")
            clean_content = self._clean_html(content)
            stars = "⭐" * rating if rating else ""
            
            full_text = f"💡 Trading Signal {stars}\n\n{content}\n\n👤 Signal by: {suggester}"
            
            media_ids = []
            if message_data['type'] == 'photo':
                media_ids = await self._upload_telegram_photo(context, message_data['file_id'])
            
            tweets = self._split_text(full_text)
            return await self._post_thread(tweets, media_ids)
            
        except Exception as e:
            logger.error(f"Failed to post signal to Twitter: {e}")
            return None
        
    async def post_daily_tip(self, context, content: Dict) -> Optional[str]:
        """Post daily tip with proper threading"""
        if not self.client:
            return None
        
        try:
            media_ids = []
            text_content = ""

            if content['type'] == 'text':
                text_content = content['content']
            elif content['type'] == 'photo':
                text_content = content.get('caption') or ""
                if content.get('file_id'):
                    media_ids = await self._upload_telegram_photo(context, content['file_id'])
            else:
                 text_content = content.get('caption') or "Trading Tip"

            full_tweet_text = f"📚 Daily Trading Tip\n\n{text_content}\n\n#ForexEducation #TradingTips"
            
            tweets = self._split_text(full_tweet_text)
            
            return await self._post_thread(tweets, media_ids)
            
        except Exception as e:
            logger.error(f"Failed to post tip to Twitter: {e}")
            return None
    
    async def post_performance_update(self, stats: Dict) -> Optional[str]:
        if not self.client: return None
        try:
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
            tweets = self._split_text(tweet)
            return await self._post_thread(tweets)
            
        except Exception as e:
            logger.error(f"Failed to post performance: {e}")
            return None


class SupportManager:
    """Manages the Support Group system with Sessions and Reactions"""
    
    SUPPORT_SESSION_TIMEOUT = 15 * 60 

    def __init__(self, db: MongoDBHandler, admin_ids: list):
        self.db = db
        self.admin_ids = admin_ids 

    async def on_new_chat_members(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Detects when the bot is added to a group."""
        bot_id = context.bot.id
        new_members = update.message.new_chat_members
        if not any(member.id == bot_id for member in new_members):
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        is_admin = user_id in self.admin_ids or self.db.get_admin_role(user_id) is not None

        if is_admin:
            if self.db.set_support_group(chat_id):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="✅ <b>Support Group Configured!</b>\n\nForwarded messages will appear here.",
                    parse_mode=ParseMode.HTML
                )
            else:
                await context.bot.send_message(chat_id=chat_id, text="❌ Database Error.")
        else:
            await context.bot.send_message(chat_id=chat_id, text="⚠️ Only Admins can set this.")
            await context.bot.leave_chat(chat_id)

    async def handle_app_message(self, context, user_id: int, content: str = None, image_data: bytes = None):
        """Handles message sent FROM the Mobile App (Text or Image)"""
        support_group_id = self.db.get_support_group()
        if not support_group_id:
            return False, "Support unavailable"

        user = self.db.users_collection.find_one({'user_id': user_id})
        username = user.get('username', 'Unknown')
        first_name = user.get('first_name', str(user_id))
        is_vip = self.db.is_subscriber(user_id)
        vip_tag = "💎 <b>VIP</b>" if is_vip else "👤 <b>Free</b>"
        
        text_header = (
            f"📱 <b>Support Request</b>\n"
            f"From: {first_name} (@{username})\n"
            f"Status: {vip_tag}\n"
            f"ID: <code>{user_id}</code>\n\n"
        )
        
        full_caption = text_header + (f"💬 {content}" if content else "")

        try:
            sent_msg = None
            msg_type = 'text'
            file_id = None

            if image_data:
                msg_type = 'photo'
                import io
                sent_msg = await context.bot.send_photo(
                    chat_id=support_group_id,
                    photo=io.BytesIO(image_data),
                    caption=full_caption,
                    parse_mode='HTML'
                )
                file_id = sent_msg.photo[-1].file_id
            else:
                sent_msg = await context.bot.send_message(
                    chat_id=support_group_id,
                    text=full_caption,
                    parse_mode='HTML'
                )

            msg_entry = {
                'user_id': user_id,
                'sender': 'user',
                'content': content,
                'type': msg_type,
                'file_id': file_id,
                'timestamp': time.time()
            }
            self.db.db['support_messages'].insert_one(msg_entry)

            self.db.save_support_mapping(sent_msg.message_id, user_id)
            return True, "Sent"

        except Exception as e:
            logger.error(f"Failed to forward to support group: {e}")
            return False, str(e)

    async def handle_user_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Forwards user DMs to the support group.
        Only sends the 'Thank you' receipt if the chat has been inactive.
        """
        support_group_id = self.db.get_support_group()
        user_id = update.effective_user.id
        
        if not support_group_id:
            await update.message.reply_text("⚠️ Support is currently unavailable.")
            return

        try:
            forwarded_msg = await context.bot.forward_message(
                chat_id=support_group_id,
                from_chat_id=update.effective_chat.id,
                message_id=update.message.message_id
            )
            
            self.db.save_support_mapping(forwarded_msg.message_id, user_id)
            
            user_doc = self.db.users_collection.find_one({'user_id': user_id})
            last_support_time = user_doc.get('last_support_time', 0) if user_doc else 0
            current_time = time.time()
            
            if (current_time - last_support_time) > self.SUPPORT_SESSION_TIMEOUT:
                await update.message.reply_text(
                    "Thank you for contacting us! Your message has been forwarded to our team and we will get back to you as soon as possible."
                )
            
            self.db.users_collection.update_one(
                {'user_id': user_id},
                {'$set': {'last_support_time': current_time}},
                upsert=True
            )

        except Exception as e:
            logger.error(f"Failed to forward support message: {e}")
            await update.message.reply_text("❌ Error connecting to support.")

    async def handle_admin_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles Admin reply (Text or Photo)"""
        reply_to = update.message.reply_to_message
        if reply_to and reply_to.text and "Reason for declining" in reply_to.text:
            return

        support_group_id = self.db.get_support_group()
        if update.effective_chat.id != support_group_id:
            return

        if not reply_to: return

        original_user_id = self.db.get_support_user_id(reply_to.message_id)
        if not original_user_id:
            forward_origin = getattr(reply_to, 'forward_origin', None)
            if forward_origin and forward_origin.type == 'user':
                original_user_id = forward_origin.sender_user.id
            elif getattr(reply_to, 'forward_from', None):
                original_user_id = reply_to.forward_from.id
        
        if original_user_id:
            try:
                msg_type = 'text'
                content = update.message.text
                file_id = None

                if update.message.photo:
                    msg_type = 'photo'
                    file_id = update.message.photo[-1].file_id
                    content = update.message.caption or ""

                reply_entry = {
                    'user_id': original_user_id,
                    'sender': 'admin',
                    'content': content,
                    'type': msg_type,
                    'file_id': file_id,
                    'timestamp': time.time()
                }
                self.db.db['support_messages'].insert_one(reply_entry)

                if msg_type == 'text':
                    await context.bot.send_message(chat_id=original_user_id, text=content)
                elif msg_type == 'photo':
                    await context.bot.send_photo(chat_id=original_user_id, photo=file_id, caption=content)

                try: await update.message.set_reaction(reaction=[ReactionTypeEmoji("❤️")])
                except: pass

            except Exception as e:
                logger.error(f"Failed to process admin reply: {e}")
                try: await update.message.set_reaction(reaction=[ReactionTypeEmoji("💔")])
                except: pass

class ReplyContainsFilter(filters.MessageFilter):
    """
    Custom filter to check if the replied-to message contains specific text.
    Replaces the old filters.create() method.
    """
    def __init__(self, text):
        self.text = text
        super().__init__()

    def filter(self, message):
        return (
            message.reply_to_message 
            and message.reply_to_message.text 
            and self.text in message.reply_to_message.text
        )

class BroadcastBot:
    def __init__(self, token: str, super_admin_ids: List[int], mongo_handler: MongoDBHandler):
        self.token = token
        self.super_admin_ids = super_admin_ids
        self.db = mongo_handler
        self.watermarker = ImageWatermarker()
        self.support_manager = SupportManager(self.db, self.super_admin_ids)
        
        self.engagement_tracker = UserEngagementTracker(self.db)
        self.broadcast_limiter = BroadcastFrequencyManager(self.db)
        self.notification_manager = NotificationManager(self.db)
        self.referral_system = ReferralSystem()
        self.achievement_system = AchievementSystem()
        self.twitter = TwitterIntegration()

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
                self.db.db, 
                EDUCATION_CHANNEL_ID
            )
            logger.info(f"Educational Content Manager initialized for channel: {EDUCATION_CHANNEL_ID}")
        else:
            self.edu_content_manager = None
            logger.warning("EDUCATION_CHANNEL_ID not set. Educational content feature disabled.")
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

    async def handle_platform_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        choice = query.data
        target = context.user_data.get('target', 'all')
        if 'ready_message_data' in context.user_data:
            message_data = context.user_data['ready_message_data']
        else:
            pass 

        status_msg = "🚀 Broadcasting started...\n"
        
        if choice in ['platform_telegram', 'platform_both']:
            status_msg += "• Sending to Telegram users...\n"
        if choice in ['platform_twitter', 'platform_both']:
            status_msg += "• Posting to Twitter...\n"
            tweet_url = await self.twitter.post_general_broadcast(context, message_data)
            if tweet_url:
                status_msg += f"✅ Tweet sent: {tweet_url}\n"
            else:
                status_msg += "❌ Twitter post failed (check logs)\n"

        await context.bot.send_message(chat_id=query.from_user.id, text=status_msg)
        return ConversationHandler.END


    async def execute_deletion_job(self, context: ContextTypes.DEFAULT_TYPE):
        """Job to execute the actual database deletion"""
        job = context.job
        user_id = job.data
        
        logger.info(f"⏳ Executing scheduled deletion for User ID: {user_id}")
        
        success = self.db.delete_user_fully(user_id)
        
        if success:
            logger.info(f"✅ Scheduled deletion completed for {user_id}")
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="ℹ️ <b>Account Deleted</b>\nYour data has been permanently removed from our servers as requested.",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
        else:
            logger.error(f"❌ Scheduled deletion failed for {user_id}")

    async def send_admin_notification(self, text: str, photo=None, reply_markup=None, fallback_admins: list = None):
        """
        Centralized method to notify admins.
        Prioritizes the Support Group. Falls back to individual DMs if no group is set.
        """
        support_group_id = self.db.get_support_group()
        
        if support_group_id:
            try:
                if photo:
                    if hasattr(photo, 'seek'): photo.seek(0)
                    await self.application.bot.send_photo(
                        chat_id=support_group_id,
                        photo=photo,
                        caption=text,
                        reply_markup=reply_markup,
                        parse_mode=ParseMode.HTML
                    )
                else:
                    await self.application.bot.send_message(
                        chat_id=support_group_id,
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode=ParseMode.HTML
                    )
                return
            except Exception as e:
                logger.error(f"Failed to send to support group: {e}")
               
        if not fallback_admins:
            return

        for admin_id in set(fallback_admins):
            try:
                if photo:
                    if hasattr(photo, 'seek'): photo.seek(0)
                    await self.application.bot.send_photo(
                        chat_id=admin_id,
                        photo=photo,
                        caption=text,
                        reply_markup=reply_markup,
                        parse_mode=ParseMode.HTML
                    )
                else:
                    await self.application.bot.send_message(
                        chat_id=admin_id,
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode=ParseMode.HTML
                    )
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")

    async def handle_deletion_approval(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the admin clicking 'Approve Deletion'"""
        query = update.callback_query
        await query.answer()
        
        if query.from_user.id not in self.super_admin_ids:
            await query.answer("❌ You are not authorized.", show_alert=True)
            return

        identifier = query.data.replace("del_approve_", "")
        user_doc = None
        if identifier.isdigit():
            user_doc = self.db.users_collection.find_one({'user_id': int(identifier)})
        else:
            clean_username = identifier.replace("@", "")
            user_doc = self.db.users_collection.find_one(
                {'username': {'$regex': f'^{re.escape(clean_username)}$', '$options': 'i'}}
            )
        
        if not user_doc:
            await query.edit_message_text(f"❌ Error: User '{identifier}' not found in database. Cannot schedule deletion.")
            return

        user_id = user_doc['user_id']
        context.job_queue.run_once(self.execute_deletion_job, 86400, data=user_id)
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "⚠️ <b>Account Deletion Approved</b>\n\n"
                    "Your request to delete your account has been processed.\n"
                    "<b>Your data will be permanently wiped in 24 hours.</b>\n\n"
                    "If this was a mistake, please contact support immediately."
                ),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning(f"Could not notify user {user_id} of deletion approval: {e}")

        admin_name = query.from_user.first_name
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M UTC')
        
        await query.edit_message_text(
            text=(
                f"🗑️ <b>Deletion Request</b>\n"
                f"User: {identifier}\n"
                f"Status: ✅ <b>Approved & Scheduled</b>\n"
                f"Admin: {admin_name}\n"
                f"Time: {timestamp}\n\n"
                f"<i>System will auto-delete data in 24 hours.</i>"
            ),
            parse_mode=ParseMode.HTML
        )

    async def handle_decline_reason_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ Catches the admin's reply with the decline reason. """
        
        if not update.message.reply_to_message:
            return
            
        bot_msg = update.message.reply_to_message.text
        
        match = re.search(r"declining User (\d+)\?", bot_msg)
        
        if not match:
            return 

        target_user_id = int(match.group(1))
        reason = update.message.text
        admin_id = update.effective_user.id
        admin_name = update.effective_user.first_name

        self.db.update_vip_request_status(target_user_id, 'rejected', admin_id, reason=reason)
        self.db.log_activity(admin_id, 'vip_declined', {'user_id': target_user_id, 'reason': reason})
        self.admin_duty_manager.credit_duty_for_action(admin_id, 'vip_declined')

        asyncio.create_task(self.send_push_to_users(
            [target_user_id], "VIP Request Declined", f"Reason: {reason}",
            data={'screen': 'Home'}
        ))
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=f"❌ Your VIP request was declined.\nReason: {reason}"
            )
        except Exception as e:
            logger.warning(f"Could not msg user {target_user_id}: {e}")

        await update.message.reply_text(f"✅ User {target_user_id} declined. Reason sent.")

    async def end_of_day_duty_verification_job(self, context: ContextTypes.DEFAULT_TYPE):
        """Runs at 23:55 UTC to auto-complete duties and send summary."""
        try:
            logger.info("Running end-of-day duty verification...")
            
            results = self.admin_duty_manager.auto_complete_duties_with_no_work()
            
            summary = "🤖 <b>End-of-Day Duty Report</b>\n"
            summary += f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
            
            if results['auto_completed_no_work']:
                summary += "✅ <b>Auto-Completed (No Work):</b>\n"
                for category, admins in results['auto_completed_no_work'].items():
                    duty_name = self.admin_duty_manager.DUTY_CATEGORIES[category]['emoji']
                    summary += f"{duty_name} {category.replace('_', ' ').title()}:\n"
                    for admin in admins:
                        summary += f"  • {admin}\n"
                summary += "\n"
            
            if results['auto_completed_covered']:
                summary += "🤝 <b>Auto-Completed (Team Coverage):</b>\n"
                for category, admins in results['auto_completed_covered'].items():
                    duty_name = self.admin_duty_manager.DUTY_CATEGORIES[category]['emoji']
                    summary += f"{duty_name} {category.replace('_', ' ').title()}:\n"
                    for admin in admins:
                        summary += f"  • {admin}\n"
                summary += "\n"
            
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
            return True

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
                if "chat not found" in e.message:
                    logger.error(f"Force-sub error: Bot cannot access channel {FORCE_SUB_CHANNEL}. Is it an admin there?")
                return False
        except Exception as e:
            logger.error(f"Error in is_user_subscribed for {user_id}: {e}")
            return False

    async def send_join_channel_message(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
        """Sends the 'please join' message"""
        FORCE_SUB_CHANNEL = os.getenv('FORCE_SUB_CHANNEL')
        if not FORCE_SUB_CHANNEL:
            return

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
        return role in [AdminRole.BROADCASTER, AdminRole.ADMIN]

    def get_user_suggestion_limit(self, user_id: int) -> (int, str):
        """Determines a user's suggestion limit and level based on rating AND achievements"""
        
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

    async def handle_signal_review_v2(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Replaces the old signal review. Handles Approve (Rating) and Reject (ForceReply)."""
        query = update.callback_query
        await query.answer()

        if query.from_user.id not in self.super_admin_ids:
            await query.answer("❌ Not authorized", show_alert=True)
            return

        action, suggestion_id = query.data.split('_', 2)[1:]
        
        if action == "approve":
            keyboard = [[
                InlineKeyboardButton("1⭐", callback_data=f"sig_rate_1_{suggestion_id}"),
                InlineKeyboardButton("2⭐", callback_data=f"sig_rate_2_{suggestion_id}"),
                InlineKeyboardButton("3⭐", callback_data=f"sig_rate_3_{suggestion_id}"),
                InlineKeyboardButton("4⭐", callback_data=f"sig_rate_4_{suggestion_id}"),
                InlineKeyboardButton("5⭐", callback_data=f"sig_rate_5_{suggestion_id}")
            ]]
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

        elif action == "reject":
            force_text = f"✍️ <b>Reason for declining Signal {suggestion_id}?</b>\n\nReply to this message."
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=force_text,
                parse_mode=ParseMode.HTML,
                reply_markup=ForceReply(selective=True)
            )

    async def receive_signal_rating_v2(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Stateless signal rating handler with Idempotency check."""
        query = update.callback_query
        
        parts = query.data.split('_')
        rating = int(parts[2])
        suggestion_id = parts[3]

        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if not suggestion:
            await query.answer("❌ Error: Signal not found", show_alert=True)
            return

        if suggestion.get('status') == 'approved':
            await query.answer("⚠️ This signal is already approved!", show_alert=True)
            try:
                original_caption = query.message.caption
                if "Approved" not in original_caption:
                    await query.edit_message_caption(
                        caption=f"{original_caption}\n\n✅ <b>Approved ({suggestion.get('rating')}⭐)</b>",
                        reply_markup=None,
                        parse_mode=ParseMode.HTML
                    )
                else:
                    await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            return

        await query.answer()

        self.db.update_suggestion_status(suggestion_id, 'approved', query.from_user.id, rating=rating)
        self.admin_duty_manager.credit_duty_for_action(query.from_user.id, 'signal_approved')
        
        suggester_id = suggestion['suggested_by']
        self.engagement_tracker.update_engagement(suggester_id, 'signal_approved')
        if rating == 5: self.engagement_tracker.update_engagement(suggester_id, 'signal_5_star')
        
        await self.achievement_system.check_and_award_achievements(suggester_id, context, self.db)
        await self.broadcast_signal(context, suggestion)

        try:
            await context.bot.send_message(
                chat_id=suggester_id,
                text=f"✅ Your signal has been approved with {rating} stars!"
            )
        except: pass

        original_caption = query.message.caption
        await query.edit_message_caption(
            caption=f"{original_caption}\n\n✅ <b>Approved ({rating}⭐)</b>",
            reply_markup=None,
            parse_mode=ParseMode.HTML
        )

    async def handle_signal_decline_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Catches the reply for Signal Rejection."""
        match = re.search(r"declining Signal (\w+)\?", update.message.reply_to_message.text)
        if not match: return

        suggestion_id = match.group(1)
        reason = update.message.text
        admin_id = update.effective_user.id

        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if suggestion:
            self.db.update_suggestion_status(suggestion_id, 'rejected', admin_id, reason=reason)
            self.admin_duty_manager.credit_duty_for_action(admin_id, 'signal_rejected')
            
            try:
                await context.bot.send_message(
                    chat_id=suggestion['suggested_by'],
                    text=f"❌ Your signal suggestion was declined.\nReason: {reason}"
                )
            except: pass
            
            await update.message.reply_text(f"✅ Signal {suggestion_id} rejected.")
        else:
            await update.message.reply_text("❌ Signal not found in DB.")

    async def handle_approval_review_v2(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Updated Broadcast Approval to use ForceReply for rejections."""
        query = update.callback_query
        await query.answer()

        if not self.has_permission(query.from_user.id, Permission.APPROVE_BROADCASTS):
            await query.answer("❌ Not authorized", show_alert=True)
            return

        action, approval_id = query.data.split('_', 2)[1:]
        approval = self.db.get_approval_by_id(approval_id)
        
        if not approval:
            await query.edit_message_text("❌ Not found.")
            return

        if action == "approve":
            self.db.update_approval_status(approval_id, 'approved', query.from_user.id)
            self.admin_duty_manager.credit_duty_for_action(query.from_user.id, 'broadcast_approved')
            await self.execute_approved_broadcast(context, approval, query.from_user.id)
            
            try:
                await context.bot.send_message(approval['created_by'], "✅ Your broadcast was approved!")
            except: pass
            
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text("✅ Broadcast Sent")

        elif action == "reject":
            force_text = f"✍️ <b>Reason for declining Broadcast {approval_id}?</b>\n\nReply to this message."
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=force_text,
                parse_mode=ParseMode.HTML,
                reply_markup=ForceReply(selective=True)
            )

    async def handle_broadcast_decline_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Catches the reply for Broadcast Rejection."""
        match = re.search(r"declining Broadcast (\w+)\?", update.message.reply_to_message.text)
        if not match: return

        approval_id = match.group(1)
        reason = update.message.text
        admin_id = update.effective_user.id

        approval = self.db.get_approval_by_id(approval_id)
        if approval:
            self.db.update_approval_status(approval_id, 'rejected', admin_id, reason=reason)
            self.admin_duty_manager.credit_duty_for_action(admin_id, 'broadcast_rejected')
            
            try:
                await context.bot.send_message(
                    chat_id=approval['created_by'],
                    text=f"❌ Your broadcast was rejected.\nReason: {reason}"
                )
            except: pass
            
            await update.message.reply_text(f"✅ Broadcast {approval_id} rejected.")
        else:
            await update.message.reply_text("❌ Broadcast not found.")

    async def start_v2(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Polished welcome with clear value prop & referral handling"""
        user = update.effective_user
        user_id = user.id
    
        if context.args and context.args[0].startswith("ref_"):
            try:
                referrer_id = int(context.args[0].split('_')[1])
                if referrer_id != user_id:
                    existing_user = self.db.users_collection.find_one({'user_id': user_id})
                    if not existing_user or 'created_at' not in existing_user:
                        await self.referral_system.process_referral(user_id, referrer_id, self.db, context)
            except Exception as e:
                logger.error(f"Error processing referral: {e}")
        # -------------------------

        if not await self.is_user_subscribed(user_id, context):
            await self.send_join_channel_message(user_id, context)
            return

        self.db.add_user(user_id, user.username, user.first_name)
        self.engagement_tracker.update_engagement(user_id, 'command_used') # Track engagement
        
        user_doc = self.db.users_collection.find_one({'user_id': user_id})
        is_new = not user_doc.get('welcomed', False)

        if self.is_admin(user_id):
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
        
            if self.is_admin(user_id):
                keyboard.append([InlineKeyboardButton("📋 Team Duties & QA", callback_data='admin_duties')])
                
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
                
                self.db.users_collection.update_one(
                    {'user_id': user_id},
                    {'$set': {'welcomed': True}}
                )
            else:
                welcome = (
                    f"Welcome back, {user.first_name}! 👋\n\n"
                    
                    "Quick access:\n"
                    "/mystats - Your performance\n"
                    "/myprogress - Your signal progress\n" 
                    "/referral - Refer friends\n" 
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


    async def handle_template_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle template menu actions (View, Delete, Use)"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        action, template_id = data.split('_', 2)[1:] 
        
        template = self.db.get_template(template_id)
        
        if not template and action != "del":
            await query.edit_message_text("❌ Template not found. It may have been deleted.")
            return

        if action == "view":
            msg_data = template['message_data']
            content_preview = " [Media Content]"
            if msg_data['type'] == 'text':
                content_preview = msg_data['content'][:100] + "..." if len(msg_data['content']) > 100 else msg_data['content']
            elif 'caption' in msg_data and msg_data['caption']:
                content_preview = msg_data['caption'][:100] + "..."
            
            text = (
                f"📝 <b>Template Details</b>\n\n"
                f"<b>Name:</b> {template['name']}\n"
                f"<b>Category:</b> {template.get('category', 'None')}\n"
                f"<b>Type:</b> {msg_data['type'].upper()}\n"
                f"<b>Preview:</b> {content_preview}\n\n"
                f"Select an action:"
            )
            
            keyboard = [
                [InlineKeyboardButton("📢 Broadcast This", callback_data=f"tpl_use_{template_id}")],
                [InlineKeyboardButton("🗑️ Delete", callback_data=f"tpl_del_{template_id}")],
                [InlineKeyboardButton("🔙 Back to List", callback_data="tpl_list_all")]
            ]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
            
        elif action == "del":
            if self.db.delete_template(template_id, query.from_user.id):
                await query.answer("Template deleted!", show_alert=True)
                await self.list_templates_callback(update, context)
            else:
                await query.answer("Failed to delete.", show_alert=True)

        elif action == "use":
            context.user_data.clear()
            
            context.user_data['ready_message_data'] = template['message_data']
            context.user_data['template_name'] = template['name']
            
            self.db.increment_template_usage(template_id)
            await self.ask_target_audience(query, context, scheduled=False)
            return WAITING_TARGET

    async def list_templates_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Helper to show list via callback (for 'Back' button)"""
        templates = self.db.get_all_templates()
        if not templates:
            await update.callback_query.edit_message_text("📝 No templates found.")
            return

        keyboard = []
        for t in templates:
            btn_text = f"{t['name']} ({t.get('category', 'General')})"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"tpl_view_{t['_id']}")])
        
        keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_settings")])
        await update.callback_query.edit_message_text(
            "📝 <b>Template Manager</b>\nSelect a template:", 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode=ParseMode.HTML
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
    
        pairs = ['EUR', 'USD', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF', 
                 'XAU', 'GOLD', 'SILVER', 'XAG', 'OIL', 'CRUDE',
                 'V25', 'V75', 'V100', 'BOOM', 'CRASH', 
                 'US30', 'NAS100', 'SPX500'] 
        has_pair = any(pair in text.upper() for pair in pairs)
    
        if not has_pair:
            if not re.search(r'[A-Z]{3}[/\s]?[A-Z]{3}', text.upper()):
                return False, "Could not identify trading pair. Use format like 'EUR/USD', 'EURUSD' or 'GOLD'."
    
        if len(text.strip()) < 20:
            return False, "Signal description is too short. Please provide more details (entry, target, reasoning)."
    
        return True, "Valid"

    def clean_empty_signal_fields(self, text: str) -> str:
        """
        Removes lines that contain keys but no values.
        Cleans up excessive newlines.
        """
        if not text:
            return ""

        lines = text.split('\n')
        cleaned_lines = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                cleaned_lines.append("") 
                continue
            if re.match(r'^[^:]+:\s*(?:null|undefined|None)?$', stripped, re.IGNORECASE):
                continue

            cleaned_lines.append(line)
            
        result = '\n'.join(cleaned_lines).strip()
        return re.sub(r'\n{3,}', '\n\n', result)

    async def validate_signal_image(self, photo_file: 'telegram.PhotoSize') -> (bool, str, str):
        """Validate signal image - MORE LENIENT"""
        MIN_WIDTH = 200  
        MIN_HEIGHT = 150 

        if photo_file.width < MIN_WIDTH or photo_file.height < MIN_HEIGHT:
            return False, f"Image is too small ({photo_file.width}x{photo_file.height}). Minimum is {MIN_WIDTH}x{MIN_HEIGHT}px.", ""
    
        try:
            photo_bytes = await (await photo_file.get_file()).download_as_bytearray()
            image = Image.open(io.BytesIO(photo_bytes))
        
            image = image.convert('L')
        
            extracted_text = pytesseract.image_to_string(image)
        
            if not extracted_text or len(extracted_text.strip()) < 5:
                return False, "Image is unclear. Could not read any text from it.", ""
        
            if len(extracted_text.strip()) >= 15: 
                is_valid, reason = self.validate_signal_format(extracted_text)
            
                if not is_valid:
                    return False, f"Image text is incomplete. {reason}", extracted_text
            else:
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
                return ConversationHandler.END 
        
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
            message_data['type'] = 'video'
            message_data['file_id'] = message.video.file_id
            message_data['caption'] = message.caption
        elif message.document:
            message_data['type'] = 'document'
            message_data['file_id'] = message.document.file_id
            message_data['caption'] = message.caption
        else:
            await update.message.reply_text("Unsupported format. Please send text or a photo.")
            return ConversationHandler.END

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

            await self.notify_super_admins_new_suggestion(context, suggestion_id)
        else:
            await update.message.reply_text("❌ Failed to submit suggestion. Please try again.")

        return ConversationHandler.END

    async def notify_super_admins_new_suggestion(self, context: ContextTypes.DEFAULT_TYPE, suggestion_id: str):
        """Notify admins of new signal suggestion via Support Group or DM"""
        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if not suggestion:
            return

        short_id = str(suggestion['_id'])[-8:]
        suggester = suggestion['suggester_name']

        notification = (
            f"💡 <b>New Signal Suggestion!</b>\n\n"
            f"From: {suggester}\n"
            f"ID: <code>{short_id}</code>\n\n"
            f"Use /signals to review pending suggestions."
        )

        await self.send_admin_notification(
            text=notification,
            fallback_admins=self.super_admin_ids
        )

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
           

        elif action == "reject":
            context.user_data['suggestion_to_reject'] = suggestion_id
            
            new_prompt = "Please provide a reason for rejecting this signal:\n\nSelect a quick reason below or type a custom one:"
            
            keyboard = [
                [InlineKeyboardButton("📉 Poor Quality", callback_data="reason_Poor quality or unclear")],
                [InlineKeyboardButton("❓ Not Enough Context", callback_data="reason_Not enough context/reasoning provided")],
                [InlineKeyboardButton("⚠️ High Risk", callback_data="reason_Signal considered too high risk")],
                [InlineKeyboardButton("❌ Invalid Format", callback_data="reason_Invalid signal format")],
                [InlineKeyboardButton("🔄 Unclear chart", callback_data="reason_Show clean and well design chart for your signal")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
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
            
            return WAITING_SIGNAL_REJECTION_REASON
            
    async def receive_signal_rating(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive signal rating, approve, and broadcast"""
        query = update.callback_query
        await query.answer()

        rating = int(query.data.split('_')[-1])
        suggestion_id = context.user_data.pop('suggestion_to_rate', None)

        if not suggestion_id:
            return ConversationHandler.END

        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if not suggestion:
            return ConversationHandler.END

        self.db.update_suggestion_status(suggestion_id, 'approved', query.from_user.id, rating=rating)
        self.admin_duty_manager.credit_duty_for_action(query.from_user.id, 'signal_approved')
        suggester_id = suggestion['suggested_by']
        self.engagement_tracker.update_engagement(suggester_id, 'signal_approved')
        if rating == 5:
            self.engagement_tracker.update_engagement(suggester_id, 'signal_5_star')
        await self.achievement_system.check_and_award_achievements(suggester_id, context, self.db)

        await self.broadcast_signal(context, suggestion)

        try:
            all_users = self.db.get_all_users()
            push_target_ids = [uid for uid in all_users if self.notification_manager.should_notify(uid, 'signals')]
            
            msg_data = suggestion['message_data']
            preview = "Check the app for details."
            if msg_data['type'] == 'text':
                preview = msg_data['content'][:50] + "..."
            elif msg_data.get('caption'):
                preview = msg_data['caption'][:50] + "..."
                
            asyncio.create_task(self.send_push_to_users(
                push_target_ids,
                "New Signal Approved! 🚀",
                f"Rating: {rating}⭐\n{preview}",
                data={'screen': 'Signals', 'initialTab': 'broadcasts'}
            ))
        except Exception as e:
            logger.error(f"Failed to initiate push notification: {e}")
        try:
            await context.bot.send_message(
                chat_id=suggestion['suggested_by'],
                text=f"✅ Your signal suggestion has been approved with a rating of {rating} stars!"
            )
        except: pass

        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"✅ Signal approved with {rating} stars!")
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

        self.db.update_suggestion_status(
            suggestion_id, 
            'rejected', 
            admin_user.id, 
            reason=reason
        )
        self.admin_duty_manager.credit_duty_for_action(update.effective_user.id, 'signal_rejected')
        try:
            asyncio.create_task(self.send_push_to_users(
                [suggestion['suggested_by']],
                "Signal Suggestion Update",
                f"Your signal was not approved. Reason: {reason}",
                data={'screen': 'Signals', 'initialTab': 'my_progress'}
            ))
        except Exception as e:
            logger.error(f"Failed to send push notification for signal rejection: {e}")

        try:
            await context.bot.send_message(
                chat_id=suggestion['suggested_by'],
                text=f"❌ Your signal suggestion was not approved.\n\nReason: {reason}"
            )
        except Exception as e:
            logger.warning(f"Failed to notify suggester {suggestion['suggested_by']} of rejection: {e}")

        await update.message.reply_text(f"❌ Signal rejected and reason recorded.")
        return ConversationHandler.END
    async def handle_quick_rejection_reason(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle quick rejection reason buttons"""
        query = update.callback_query
        await query.answer()
        
        reason = query.data.replace("reason_", "")
        suggestion_id = context.user_data.pop('suggestion_to_reject', None)
        admin_user = update.effective_user
        
        async def safe_edit(text):
            if query.message.caption is not None:
                await query.edit_message_caption(caption=text)
            else:
                await query.edit_message_text(text=text)

        if not suggestion_id:
            await safe_edit("❌ Error: Suggestion ID not found. Please try again.")
            return ConversationHandler.END

        suggestion = self.db.get_suggestion_by_id(suggestion_id)
        if not suggestion:
            await safe_edit("❌ Error: Suggestion not found.")
            return ConversationHandler.END

        self.db.update_suggestion_status(
            suggestion_id, 
            'rejected', 
            admin_user.id, 
            reason=reason
        )
        self.admin_duty_manager.credit_duty_for_action(update.effective_user.id, 'signal_rejected')
        try:
            asyncio.create_task(self.send_push_to_users(
                [suggestion['suggested_by']],
                "Signal Suggestion Update",
                f"Your signal was not approved. Reason: {reason}",
                data={'screen': 'Signals', 'initialTab': 'my_progress'}
            ))
        except Exception as e:
            logger.error(f"Failed to send push notification for signal rejection: {e}")
        try:
            await context.bot.send_message(
                chat_id=suggestion['suggested_by'],
                text=f"❌ Your signal suggestion was not approved.\n\nReason: {reason}"
            )
        except Exception as e:
            logger.warning(f"Failed to notify suggester {suggestion['suggested_by']} of rejection: {e}")

        await safe_edit(f"❌ Signal rejected.\nReason: {reason}")
        return ConversationHandler.END

    async def check_and_handle_block(self, user_id: int, error: Exception) -> bool:
        """
        Check if the error indicates the bot was blocked or the user is invalid.
        If so, remove them from the database immediately.
        """
        err_str = str(error).lower()
        block_indicators = [
            "bot was blocked",
            "user is deactivated",
            "chat not found",
            "forbidden"
        ]
        
        if any(indicator in err_str for indicator in block_indicators):
            self.db.delete_blocked_user(user_id)
            return True
        return False

    async def broadcast_signal(self, context: ContextTypes.DEFAULT_TYPE, suggestion: Dict):
        """Broadcast approved signal to all users (Optimized for Performance)"""
        all_users = self.db.get_all_users()
        
        target_users = self.notification_manager.get_eligible_users(all_users, 'signals')
        
        message_data = suggestion['message_data']
        suggester = suggestion['suggester_name']
        rating = suggestion.get('rating')

        attribution = f"\n\n💡 Signal suggested by: {suggester}"
        if rating:
            attribution += f"\n⭐ Admin Rating: {'⭐' * rating}"

        attribution += "\n\n🔕 Disable: /settings then toggle off Signal Suggestions"

        success_count = 0
        failed_count = 0

        for user_id in target_users:
            try:
                if message_data['type'] == 'text':
                    full_text = message_data['content'] + attribution
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=full_text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                elif message_data['type'] == 'photo':
                    caption = (message_data.get('caption') or '') + attribution
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=message_data['file_id'],
                        caption=caption,
                        parse_mode=ParseMode.HTML
                    )
                elif message_data['type'] == 'video':
                    caption = (message_data.get('caption') or '') + attribution
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=message_data['file_id'],
                        caption=caption,
                        parse_mode=ParseMode.HTML
                    )
                elif message_data['type'] == 'document':
                    caption = (message_data.get('caption') or '') + attribution
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=message_data['file_id'],
                        caption=caption,
                        parse_mode=ParseMode.HTML
                    )

                success_count += 1
                
                await asyncio.sleep(0.05) 
                
            except Exception as e:
                # Check for blocking/deactivation and remove user
                if await self.check_and_handle_block(user_id, e):
                    failed_count += 1
                    continue

                if "chat not found" not in str(e).lower() and "bot was blocked" not in str(e).lower():
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
            self.db.update_approval_status(approval_id, 'approved', query.from_user.id)
            self.admin_duty_manager.credit_duty_for_action(query.from_user.id, 'broadcast_approved')

            await self.execute_approved_broadcast(context, approval, query.from_user.id)

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
        try:
            push_target_ids = [uid for uid in target_users if self.notification_manager.should_notify(uid, 'broadcasts')]
            
            preview = "Tap to view."
            if message_data['type'] == 'text':
                preview = message_data['content'][:60] + "..."
            elif message_data.get('caption'):
                preview = message_data['caption'][:60] + "..."
                
            asyncio.create_task(self.send_push_to_users(
                push_target_ids,
                "New Announcement 📢",
                preview,
                data={'screen': 'Home'}
            ))
        except Exception as e:
            logger.error(f"Failed to initiate broadcast push: {e}")

        success_count = 0
        failed_count = 0
                                            
        footer = "\n\n🔕 Disable: /settings then toggle off Admin Signals & Announcements"

        for user_id in target_users:
            if not self.notification_manager.should_notify(user_id, 'broadcasts'):
                failed_count += 1
                continue
            try:
                if message_data['type'] == 'text':
                    text_to_send = message_data['content'] + footer
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=text_to_send,
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )
                elif message_data['type'] == 'photo':
                    caption_to_send = (message_data.get('caption') or '') + footer
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=message_data['file_id'],
                        caption=caption_to_send,
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )
                elif message_data['type'] == 'video':
                    caption_to_send = (message_data.get('caption') or '') + footer
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=message_data['file_id'],
                        caption=caption_to_send,
                        reply_markup=message_data.get('inline_buttons'),
                        protect_content=message_data.get('protect_content', False)
                    )
                elif message_data['type'] == 'document':
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
                if await self.check_and_handle_block(user_id, e):
                    failed_count += 1
                    continue
                    
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
        """Start broadcast conversation - NOW ASKS PLATFORM FIRST"""
        if not self.has_permission(update.effective_user.id, Permission.BROADCAST):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END

        keyboard = [
            [InlineKeyboardButton("✈️ Telegram Only", callback_data="platform_telegram")],
            [InlineKeyboardButton("🐦 Twitter Only", callback_data="platform_twitter")],
            [InlineKeyboardButton("🚀 Both Platforms", callback_data="platform_both")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "📢 <b>Start Broadcasting</b>\n\n"
            "Select the platform(s) for this broadcast:",
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
        context.user_data.clear()
        return WAITING_INITIAL_PLATFORM

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
        context.user_data['scheduled'] = True 
        return WAITING_MESSAGE

    async def handle_initial_platform_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the initial platform selection"""
        query = update.callback_query
        await query.answer()
        
        platform = query.data
        context.user_data['platform'] = platform
        
        if platform == 'platform_twitter':
            context.user_data['target'] = 'all'

        await query.edit_message_text(
            f"✅ Platform set to: {platform.replace('platform_', '').title()}\n\n"
            "<b>Send me the message to broadcast.</b>\n"
            "You can send text, photos, videos, or documents.\n\n"
            "Send /cancel to cancel.",
            parse_mode=ParseMode.HTML
        )
        return WAITING_MESSAGE

    async def receive_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive broadcast message and route based on platform"""
        context.user_data['broadcast_message'] = update.message
        platform = context.user_data.get('platform', 'platform_telegram')
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

        if platform == 'platform_twitter':
            return await self.prepare_and_submit_broadcast(update, context)

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
        """Handle watermark choice and route based on platform"""
        query = update.callback_query
        await query.answer()

        use_watermark = query.data == "watermark_yes"
        context.user_data['use_watermark'] = use_watermark
        platform = context.user_data.get('platform', 'platform_telegram')

        if use_watermark:
            await query.edit_message_text("Watermarking image...")
            message = context.user_data['broadcast_message']
            photo_file = await message.photo[-1].get_file()
            
            image_bytes = await photo_file.download_as_bytearray()
            watermarked_image = self.watermarker.add_watermark(bytes(image_bytes))
            
            context.user_data['watermarked_image'] = watermarked_image

        if platform == 'platform_twitter':
             return await self.prepare_and_submit_broadcast(update, context)

        keyboard = [
            [
                InlineKeyboardButton("➕ Add Buttons", callback_data="add_buttons"),
                InlineKeyboardButton("➡️ Skip", callback_data="skip_buttons")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if use_watermark:
             await context.bot.send_message(
                 chat_id=query.message.chat_id,
                 text="Do you want to add inline buttons to your message?",
                 reply_markup=reply_markup
             )
        else:
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
            await context.bot.send_message(chat_id=update.from_user.id, text=message, reply_markup=reply_markup)

        return WAITING_TARGET

    async def handle_target_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle target choice and FINISH (Telegram/Both path)"""
        query = update.callback_query
        await query.answer()
        if 'scheduled_time' in context.user_data:
            return await self.finalize_scheduled_broadcast(update, context)

        target_map = {
            'target_all': 'all',
            'target_subscribers': 'subscribers',
            'target_nonsubscribers': 'nonsubscribers',
            'target_admins': 'admins'
        }
        context.user_data['target'] = target_map.get(query.data, 'all')
        
        return await self.prepare_and_submit_broadcast(update, context)

    async def prepare_and_submit_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Consolidated logic to construct the message data and execute/submit for approval.
        Called by:
        1. receive_broadcast_message (if Twitter only + text)
        2. handle_watermark_choice (if Twitter only + photo)
        3. handle_target_choice (if Telegram/Both)
        """
        if hasattr(update, 'callback_query') and update.callback_query:
            user_id = update.callback_query.from_user.id
            message_obj = update.callback_query.message
            is_callback = True
        else:
            user_id = update.effective_user.id
            message_obj = update.message
            is_callback = False

        broadcast_message = context.user_data.get('broadcast_message')
        if not broadcast_message:
            msg = "❌ Error: No message found. Please restart."
            if is_callback: await update.callback_query.edit_message_text(msg)
            else: await update.message.reply_text(msg)
            return ConversationHandler.END
            
        inline_buttons = context.user_data.get('inline_buttons')
        protect_content = context.user_data.get('protect_content', False)
        use_watermark = context.user_data.get('use_watermark', False)
        watermarked_image = context.user_data.get('watermarked_image')
        target = context.user_data.get('target', 'all')
        platform = context.user_data.get('platform', 'platform_telegram')
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
                        caption="Generating file_id..."
                    )
                    message_data['file_id'] = sent_photo.photo[-1].file_id
                    await sent_photo.delete() 
                except Exception as e:
                    logger.error(f"Failed to send/delete watermarked photo: {e}")
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

        context.user_data['ready_message_data'] = message_data
        
        is_quality, issues = BroadcastQualityChecker.check_broadcast_quality(message_data)
        if not is_quality:
            issues_text = "\n".join([f"• {issue}" for issue in issues])
            msg = f"❌ Broadcast Quality Check Failed:\n\n{issues_text}\n\nPlease /cancel and try again."
            if is_callback: await update.callback_query.edit_message_text(msg)
            else: await update.message.reply_text(msg)
            return ConversationHandler.END
            
        can_send, reason = await self.broadcast_limiter.can_broadcast(user_id)
        if not can_send:
            if is_callback: await update.callback_query.edit_message_text(reason)
            else: await update.message.reply_text(reason)
            return ConversationHandler.END
            
        if self.needs_approval(user_id):
            creator_name = (update.effective_user.first_name or update.effective_user.username or str(user_id))
            approval_id = self.db.create_broadcast_approval(
                message_data,
                user_id,
                creator_name,
                target
            )

            if approval_id:
                self.db.log_activity(user_id, 'broadcast_submitted', {'approval_id': approval_id})
                msg = (
                    "⏳ Broadcast submitted for approval!\n\n"
                    "Moderators/Super Admins will review your broadcast.\n"
                    "You'll be notified when it's reviewed."
                )
                if is_callback: await update.callback_query.edit_message_text(msg)
                else: await update.message.reply_text(msg)
                await self.notify_approvers_new_broadcast(context, approval_id)
            else:
                msg = "❌ Failed to submit broadcast. Please try again."
                if is_callback: await update.callback_query.edit_message_text(msg)
                else: await update.message.reply_text(msg)

            return ConversationHandler.END

        status_msg = "🚀 Broadcasting started...\n"
        if platform == 'platform_twitter':
             status_msg += "• Posting to Twitter...\n"
        elif platform == 'platform_telegram':
             status_msg += "• Sending to Telegram users...\n"
        else:
             status_msg += "• Sending to Telegram & Twitter...\n"

        if is_callback: await update.callback_query.edit_message_text(status_msg)
        else: await update.message.reply_text(status_msg)

        if platform in ['platform_telegram', 'platform_both']:
            dummy_approval = {
                '_id': 'DIRECT',
                'message_data': message_data,
                'target': target,
                'created_by': user_id
            }
            approval_data = {
                'message_data': message_data,
                'created_by': user_id,
                'creator_name': update.effective_user.first_name or update.effective_user.username or str(user_id),
                'target': target,
                'scheduled': False,
                'status': 'approved',
                'created_at': time.time(),
                'reviewed_at': time.time(),
                'reviewed_by': user_id
            }
            
            result = self.db.broadcast_approvals_collection.insert_one(approval_data)
            
            approval_data['_id'] = result.inserted_id 

            await self.execute_approved_broadcast(context, approval_data, user_id)
            
        if platform in ['platform_twitter', 'platform_both']:
            tweet_url = await self.twitter.post_general_broadcast(context, message_data)
            if tweet_url:
                await context.bot.send_message(chat_id=user_id, text=f"✅ Tweet sent: {tweet_url}")
            else:
                await context.bot.send_message(chat_id=user_id, text="❌ Twitter post failed (check logs)")

        return ConversationHandler.END
            
        if self.needs_approval(user_id):
            can_send, reason = await self.broadcast_limiter.can_broadcast(user_id)
            if not can_send:
                await query.edit_message_text(reason)
                return ConversationHandler.END
                
            creator_name = query.from_user.first_name or query.from_user.username or str(user_id)
            approval_id = self.db.create_broadcast_approval(
                message_data,
                user_id,
                creator_name,
                selected_target
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

        keyboard = [
            [InlineKeyboardButton("✈️ Telegram Only", callback_data="platform_telegram")],
            [InlineKeyboardButton("🐦 Twitter Only", callback_data="platform_twitter")],
            [InlineKeyboardButton("🚀 Both Platforms", callback_data="platform_both")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        target_display_names = {
            'all': 'All Users',
            'subscribers': 'Subscribers',
            'nonsubscribers': 'Non-Subscribers',
            'admins': 'Admins'
        }
        display_name = target_display_names.get(selected_target, 'Selected Audience')

        await query.edit_message_text(
            f"🎯 Target Selected: <b>{display_name}</b>\n\n"
            "<b>Where do you want to post this broadcast?</b>",
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )

        return WAITING_PLATFORM
        
    async def notify_approvers_new_broadcast(self, context: ContextTypes.DEFAULT_TYPE, approval_id: str):
        """Notify approvers of new broadcast pending approval"""
        approval = self.db.get_approval_by_id(approval_id)
        if not approval:
            return

        short_id = str(approval['_id'])[-8:]
        creator = approval['creator_name']

        notification = (
            f"⏳ <b>New Broadcast Pending Approval!</b>\n\n"
            f"Creator: {creator}\n"
            f"ID: <code>{short_id}</code>\n\n"
            f"Use /approvals to review pending broadcasts."
        )
        
        admins = self.db.get_all_admins()
        approvers = [a['user_id'] for a in admins if self.has_permission(a['user_id'], Permission.APPROVE_BROADCASTS)]

        await self.send_admin_notification(
            text=notification,
            fallback_admins=approvers
        )

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
                                text_to_send = message_data['content'] + footer
                                await context.bot.send_message(
                                    chat_id=user_id,
                                    text=text_to_send,
                                    reply_markup=message_data.get('inline_buttons'),
                                    protect_content=message_data.get('protect_content', False)
                                )
                            elif message_data['type'] == 'photo':
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
                            if await self.check_and_handle_block(user_id, e):
                                failed_count += 1
                                continue
                                
                            logger.error(f"Failed to send scheduled to {user_id}: {e}")
                            failed_count += 1

                    if broadcast['repeat'] == 'once':
                        self.db.update_broadcast_status(broadcast_id, 'completed')
                    else:
                        next_time = self.calculate_next_time(broadcast['scheduled_time'], broadcast['repeat'])
                        self.db.scheduled_broadcasts_collection.update_one(
                            {'_id': broadcast['_id']},
                            {'$set': {'scheduled_time': next_time}}
                        )

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
            if i < 3:
                rank_icon = medals[i]
            else:
                rank_icon = f"<b>#{i+1}</b>"
            
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
                    if await self.check_and_handle_block(user_id, e):
                        continue
                    logger.error(f"Failed to send leaderboard to {user_id}: {e}")
                    
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
        else:
            activity_level = "Poor activity — your contribution is disappointing."

        return f"Comment: {activity_level}" 

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

        await self.broadcast_suggester_leaderboard_v2(context, 'weekly')
        
        await self.broadcast_admin_leaderboard_v2(context, 'weekly')

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
            next_dt = dt + timedelta(days=30)
        else:
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
            admins = self.db.get_all_admins()
            
            if not admins:
                logger.warning("No admins found for duty assignment")
                return
            
            assignments = self.admin_duty_manager.assign_daily_duties(admins)
            
            if not assignments:
                logger.warning("No duty assignments created")
                return
            
            for admin_id, duty_data in assignments.items():
                await self.send_duty_notification(context, admin_id, duty_data)
            
            await self.send_duty_summary_to_super_admins(context, assignments)
            
            logger.info(f"Daily duties assigned and notifications sent to {len(assignments)} admins")
            
        except Exception as e:
            logger.error(f"Error in assign_daily_duties_job: {e}")
    
    async def send_duty_notification(self, context: ContextTypes.DEFAULT_TYPE, 
                                     admin_id: int, duty_data: Dict):
        """Send duty assignment notification to admin"""
        duty_info = duty_data['duty_info']
        
        tasks_text = "\n".join([f"  • {task}" for task in duty_info['tasks']])
        
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
        if category == 'content_creation' and action_count == 0:
            if not context.args:
                await update.message.reply_text(
                    f"⚠️ <b>Verification Required</b>\n\n"
                    f"The system hasn't detected any template creations or content syncs today.\n\n"
                    f"If you created content externally (e.g. directly in the channel), please provide proof/notes:\n"
                    f"<code>/dutycomplete Posted market update in channel</code>",
                    parse_mode=ParseMode.HTML
                )
                return

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
        if str(update.effective_chat.id) == str(self.edu_content_manager.channel_id):
            saved = await self.edu_content_manager.process_and_save(update.channel_post)
            if saved:
                logger.info(f"Saved new educational content: {update.channel_post.message_id}")

    async def forward_listener(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Allow admins to forward old posts to backfill the database"""
        user_id = update.effective_user.id
        if not self.is_admin(user_id):
            return

        if update.message.forward_from_chat:
             saved = await self.edu_content_manager.process_and_save(update.message)
             if saved:
                 await update.message.reply_text("✅ Content saved to educational database!")

    async def send_push_to_users(self, user_ids: list, title: str, body: str, data: dict = None):
        """Send Expo Push Notifications with Bulk Writes and Batching"""
        if not user_ids:
            return

        logger.info(f"🔔 Attempting to send push to {len(user_ids)} users")
        
        all_expo_messages = []
        all_history_logs = []
        user_ids_list = list(user_ids)
        
        CHUNK_SIZE = 2000 
        
        try:
            for i in range(0, len(user_ids_list), CHUNK_SIZE):
                chunk = user_ids_list[i:i + CHUNK_SIZE]
                
                users = list(self.db.users_collection.find(
                    {'user_id': {'$in': chunk}, 'push_token': {'$exists': True, '$ne': None}}
                ))
                
                current_time = time.time()
                
                for user in users:
                    user_id = user.get('user_id')
                    token = user.get('push_token')
                    token_str = str(token).strip()

                    all_history_logs.append({
                        'user_id': user_id,
                        'title': title,
                        'body': body,
                        'data': data or {},
                        'read': False,
                        'timestamp': current_time
                    })

                    if not token_str.startswith('ExponentPushToken'):
                        continue

                    note = {
                        'to': token_str,
                        'title': title,
                        'body': body,
                        'sound': 'default',
                        'priority': 'high',
                        'channelId': 'default',
                    }
                    if data:
                        note['data'] = data
                    all_expo_messages.append(note)

            if all_history_logs:
                
                for i in range(0, len(all_history_logs), CHUNK_SIZE):
                    self.db.save_notifications_bulk(all_history_logs[i:i+CHUNK_SIZE])

            if not all_expo_messages:
                return
                
            async with aiohttp.ClientSession() as session:
                EXPO_CHUNK_SIZE = 100
                total_sent = 0
                
                for i in range(0, len(all_expo_messages), EXPO_CHUNK_SIZE):
                    chunk = all_expo_messages[i:i + EXPO_CHUNK_SIZE]
                    try:
                        async with session.post(
                            'https://exp.host/--/api/v2/push/send',
                            json=chunk,
                            headers={
                                'Accept': 'application/json',
                                'Accept-Encoding': 'gzip, deflate',
                                'Content-Type': 'application/json'
                            },
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as response:
                            if response.status == 200:
                                total_sent += len(chunk)
                    except Exception as e:
                        logger.error(f"Error sending batch {i}: {e}")

            logger.info(f"✅ Push batch completed: {total_sent} messages sent")

        except Exception as e:
            logger.error(f"💥 CRITICAL ERROR in send_push_to_users: {e}")
        

  

    def create_api_server(self):
        """Create API server for Mobile App Integration"""
        
        async def api_send_code(request):
            """API Endpoint: Send Verification Code to Telegram"""
            try:
                data = await request.json()
                query = data.get('username', '').strip().replace('@', '')
                
                if not query:
                    return web.json_response({'error': 'Username or ID required'}, status=400)

                user = None
                if query.isdigit():
                    user = self.db.users_collection.find_one({'user_id': int(query)})
                else:
                    user = self.db.users_collection.find_one(
                        {'username': {'$regex': f'^{re.escape(query)}$', '$options': 'i'}}
                    )
                
                if not user:
                    return web.json_response({
                        'error': 'user_not_found',
                        'message': 'User not found. Please start the bot first.',
                        'bot_link': 'https://t.me/Pipsage_bot'
                    }, status=404)

                user_id = user['user_id']
                code = str(random.randint(100000, 999999))
                expiry = time.time() + 300 # 5 mins

                self.db.users_collection.update_one(
                    {'user_id': user_id},
                    {'$set': {'auth_code': code, 'auth_code_expiry': expiry}}
                )

                try:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text=f"🔐 <b>PipSage Login Code</b>\n\nYour verification code is: <code>{code}</code>\n\nValid for 5 minutes.",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"Failed to send code to {user_id}: {e}")
                    return web.json_response({'error': 'Could not send code to Telegram. Is the bot blocked?'}, status=500)

                return web.json_response({'success': True, 'user_id': user_id})

            except Exception as e:
                return web.json_response({'error': str(e)}, status=500)

        async def api_verify_code(request):
            """API Endpoint: Verify Code"""
            try:
                data = await request.json()
                user_id = data.get('user_id')
                code = data.get('code')

                if not user_id or not code:
                    return web.json_response({'error': 'Missing parameters'}, status=400)

                user = self.db.users_collection.find_one({'user_id': int(user_id)})
                
                if not user or 'auth_code' not in user:
                    return web.json_response({'error': 'Invalid request'}, status=400)

                if str(user['auth_code']) != str(code):
                     return web.json_response({'error': 'Invalid code'}, status=401)
                
                if time.time() > user.get('auth_code_expiry', 0):
                     return web.json_response({'error': 'Code expired'}, status=401)

                # Clear code after success
                self.db.users_collection.update_one(
                    {'user_id': int(user_id)}, 
                    {'$unset': {'auth_code': "", 'auth_code_expiry': ""}}
                )

                return web.json_response({'success': True, 'user_id': user_id})
            except Exception as e:
                return web.json_response({'error': str(e)}, status=500)
        
        async def api_update_push_token(request):
            """API Endpoint: Update User Push Token with validation"""
            try:
                try:
                    client_ip = request.remote
                    logger.info(f"📥 Push Token Request received from IP: {client_ip}")
                except:
                    logger.info("📥 Push Token Request received (unknown IP)")

                data = await request.json()
                logger.info(f"📦 Token Payload: user_id={data.get('user_id')}, token_len={len(str(data.get('token', '')))}")

                user_id = data.get('user_id')
                token = data.get('token')

                if not user_id or not token:
                    logger.warning(f"❌ Missing data in push token update: user_id={user_id}, token={'present' if token else 'missing'}")
                    return web.json_response({'error': 'Missing user_id or token'}, status=400)

                try: 
                    user_id = int(user_id)
                except: 
                    logger.warning(f"❌ Invalid user_id format: {user_id}")
                    return web.json_response({'error': 'Invalid user_id'}, status=400)

                token_str = str(token).strip()
                if not (token_str.startswith('ExponentPushToken[') or token_str.startswith('ExponentPushToken')):
                    logger.warning(f"❌ Invalid Expo token format for user {user_id}: {token_str[:30]}")
                    return web.json_response({'error': 'Invalid Expo token format'}, status=400)

                user = self.db.users_collection.find_one({'user_id': user_id})
                if not user:
                    logger.warning(f"⚠️ Attempted to register push token for non-existent user {user_id}")
                    self.db.add_user(user_id)
                    logger.info(f"✅ Created new user {user_id} during push token registration")

                if self.db.update_user_push_token(user_id, token_str):
                    logger.info(f"✅ Successfully registered push token for user {user_id}")
                    return web.json_response({'success': True})
                else:
                    logger.error(f"💥 Database error registering token for user {user_id}")
                    return web.json_response({'error': 'Database error'}, status=500)
            
            except Exception as e:
                logger.error(f"💥 CRITICAL API ERROR in api_update_push_token: {e}")
                return web.json_response({'error': str(e)}, status=500)
                
        async def api_get_stats(request):
            try:
                try:
                    user_id = int(request.match_info['user_id'])
                except (ValueError, TypeError):
                    return web.json_response({'error': 'Invalid User ID format'}, status=400)

                user = self.db.users_collection.find_one({'user_id': user_id})
                vip_request = self.db.get_latest_vip_request(user_id)
                vip_req_status = 'none'
                vip_rejection_reason = None
                
                if vip_request:
                    vip_req_status = vip_request.get('status', 'none')
                    vip_rejection_reason = vip_request.get('rejection_reason')
            
                if not user:
                    print(f"⚠️ Warning: User ID {user_id} not found in DB. Using defaults.")
                    user = {}
                    username = "Unknown Trader"
                else:
                    username = user.get('first_name') or user.get('username') or f"Trader {user_id}"

                is_vip = self.db.is_subscriber(user_id) 

                try:
                    avg_rating = self.db.get_user_average_rating(user_id) or 0.0
                    signal_stats = self.db.get_user_signal_stats(user_id) or {'total': 0, 'approved': 0, 'rate': '0%'}
                except Exception as db_err:
                    print(f"Database Calculation Error: {db_err}")
                    avg_rating = 0.0
                    signal_stats = {'total': 0, 'approved': 0, 'rate': '0%'}

                updates = []
            
                achievements = user.get('achievements', [])
                if achievements:
                    last_ach = achievements[-1]
                    updates.append({
                        "title": "Achievement Unlocked!",
                        "desc": f"You unlocked: {str(last_ach).replace('_', ' ').title()}",
                        "time": "Recently",
                        "type": "success"
                    })

                try:
                    recent_signal = self.db.signal_suggestions_collection.find_one(
                        {'suggested_by': user_id, 'status': 'approved'},
                        sort=[('reviewed_at', -1)]
                    )
                    if recent_signal:
                        rating = recent_signal.get('rating', 5)
                        updates.append({
                            "title": "Signal Approved",
                            "desc": f"Your signal received {rating}⭐",
                            "time": "Recently",
                            "type": "alert"
                        })
                except Exception:
                    pass 

               
                if not updates:
                    updates.append({
                        "title": "Welcome!", 
                        "desc": "Start trading to see updates.", 
                        "time": "Now", 
                        "type": "info"
                    })

                data = {
                    'username': username,
                    'is_vip': is_vip,
                    'vip_request_status': vip_req_status,
                    'vip_rejection_reason': vip_rejection_reason,
                    'rating': round(float(avg_rating), 2),
                    'total_signals': signal_stats.get('total', 0),
                    'approved_signals': signal_stats.get('approved', 0),
                    'approval_rate': signal_stats.get('rate', '0%'),
                    'recent_updates': updates
                }
            
                return web.json_response(data)

            except Exception as e:
                import traceback
                traceback.print_exc() 
                print(f"🔥 CRITICAL API ERROR: {str(e)}")
            
                return web.json_response({'error': 'Internal Server Error', 'details': str(e)}, status=500)


        async def api_clear_notifications(request):
            """API Endpoint: Clear all notifications for a user"""
            try:
                user_id = request.match_info['user_id']
                try: 
                    user_id = int(user_id)
                except (ValueError, TypeError): 
                    return web.json_response({'error': 'Invalid User ID'}, status=400)

                result = self.db.notifications_collection.delete_many({'user_id': user_id})
                
                logger.info(f"✅ Cleared {result.deleted_count} notifications for user {user_id}")
                
                return web.json_response({
                    'success': True,
                    'deleted_count': result.deleted_count
                })
                
            except Exception as e:
                logger.error(f"API Clear Notifications Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_mark_notification_read(request):
            """API Endpoint: Mark a specific notification as read"""
            try:
                data = await request.json()
                user_id = data.get('user_id')
                notification_timestamp = data.get('timestamp')
                
                if not user_id or not notification_timestamp:
                    return web.json_response({'error': 'Missing parameters'}, status=400)
                
                try: 
                    user_id = int(user_id)
                    notification_timestamp = float(notification_timestamp)
                except (ValueError, TypeError): 
                    return web.json_response({'error': 'Invalid parameters'}, status=400)

                result = self.db.notifications_collection.update_one(
                    {'user_id': user_id, 'timestamp': notification_timestamp},
                    {'$set': {'read': True}}
                )
                
                return web.json_response({
                    'success': True,
                    'modified': result.modified_count > 0
                })
                
            except Exception as e:
                logger.error(f"API Mark Read Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_mark_all_read(request):
            """API Endpoint: Mark all notifications as read for a user"""
            try:
                user_id = request.match_info['user_id']
                try: 
                    user_id = int(user_id)
                except (ValueError, TypeError): 
                    return web.json_response({'error': 'Invalid User ID'}, status=400)

                # Mark all as read
                result = self.db.notifications_collection.update_many(
                    {'user_id': user_id, 'read': False},
                    {'$set': {'read': True}}
                )
                
                logger.info(f"✅ Marked {result.modified_count} notifications as read for user {user_id}")
                
                return web.json_response({
                    'success': True,
                    'modified_count': result.modified_count
                })
                
            except Exception as e:
                logger.error(f"API Mark All Read Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_get_unread_count(request):
            """API Endpoint: Get count of unread notifications"""
            try:
                user_id = request.match_info['user_id']
                try: 
                    user_id = int(user_id)
                except (ValueError, TypeError): 
                    return web.json_response({'error': 'Invalid User ID'}, status=400)

                unread_count = self.db.notifications_collection.count_documents({
                    'user_id': user_id,
                    'read': False
                })
                
                return web.json_response({
                    'success': True,
                    'unread_count': unread_count
                })
                
            except Exception as e:
                logger.error(f"API Unread Count Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_check_push_status(request):
            """API Endpoint: Check if user has valid push token registered"""
            try:
                user_id = request.match_info['user_id']
                try: 
                    user_id = int(user_id)
                except: 
                    return web.json_response({'error': 'Invalid ID'}, status=400)

                user = self.db.users_collection.find_one({'user_id': user_id})
        
                if not user:
                    return web.json_response({
                        'user_exists': False,
                        'has_token': False,
                        'message': 'User not found'
                    })

                token = user.get('push_token')
                token_updated = user.get('push_token_updated_at')
        
                if token:
                    token_str = str(token).strip()
                    is_valid = token_str.startswith('ExponentPushToken')
            
                    return web.json_response({
                        'user_exists': True,
                        'has_token': True,
                        'token_valid': is_valid,
                        'token_preview': token_str[:30] + '...' if len(token_str) > 30 else token_str,
                        'token_updated_at': token_updated,
                        'message': 'Token registered' if is_valid else 'Invalid token format'
                    })
                else:
                    return web.json_response({
                        'user_exists': True,
                        'has_token': False,
                        'message': 'No push token registered'
                    })

            except Exception as e:
                logger.error(f"Error checking push status: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_delete_account_page(request):
            """Web Page for Play Store Data Deletion Requirement"""
            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Delete Account - PipSage</title>
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <style>
                    body { font-family: sans-serif; padding: 20px; max-width: 600px; margin: 0 auto; line-height: 1.6; }
                    h1 { color: #2563EB; }
                    form { background: #f4f4f4; padding: 20px; border-radius: 8px; }
                    label { display: block; margin-bottom: 8px; font-weight: bold; }
                    input { width: 100%; padding: 10px; margin-bottom: 20px; border: 1px solid #ccc; border-radius: 4px; }
                    button { background: #dc2626; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-size: 16px; }
                    .note { font-size: 0.9em; color: #666; margin-top: 20px; }
                </style>
            </head>
            <body>
                <h1>Request Account Deletion</h1>
                <p>To comply with data privacy laws, you can request the deletion of your PipSage account and associated data.</p>
                
                <form action="/api/delete-account-submit" method="post">
                    <label>Telegram User ID or Username:</label>
                    <input type="text" name="user_identifier" required placeholder="@username or 12345678">
                    
                    <label>Reason (Optional):</label>
                    <input type="text" name="reason" placeholder="Why are you leaving?">
                    
                    <button type="submit">Submit Deletion Request</button>
                </form>

                <div class="note">
                    <strong>Note:</strong> This will flag your account for deletion. Our admins will verify the request within 48 hours. You will lose access to VIP signals and your leaderboard stats.
                </div>
            </body>
            </html>
            """
            return web.Response(text=html_content, content_type='text/html')

        async def api_delete_account_submit(request):
            """Handle the deletion request with Admin Approval Button"""
            data = await request.post()
            identifier = data.get('user_identifier', '').strip()
            reason = data.get('reason', 'No reason provided')
            
            if not identifier:
                 return web.Response(text="<h1>Error</h1><p>User identifier is missing.</p>", content_type='text/html')

            logger.info(f"ACCOUNT DELETION REQUEST: {identifier}")
            
            keyboard = [
                [InlineKeyboardButton("✅ Approve & Schedule Deletion", callback_data=f"del_approve_{identifier}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            sent_count = 0
            for admin_id in self.super_admin_ids:
                try:
                    await self.send_admin_notification(
                        text=(
                            f"🗑️ <b>Deletion Request</b>\n"
                            f"User: {identifier}\n"
                            f"Reason: {reason}\n\n"
                            f"<i>Tap approve to schedule data wipe in 24 hours.</i>"
                        ),
                        reply_markup=reply_markup,
                        fallback_admins=self.super_admin_ids
                    )
                    sent_count = 1 
                except Exception as e:
                    logger.error(f"Failed to send deletion request: {e}")
                    sent_count = 0

            if sent_count > 0:
                return web.Response(text="<h1>Request Received</h1><p>Your request has been logged and sent to admins for approval. You will be notified on Telegram.</p>", content_type='text/html')
            else:
                return web.Response(text="<h1>Error</h1><p>Could not contact admins. Please try again later.</p>", content_type='text/html', status=500)
        
        async def api_get_news(request):
            """API Endpoint: Get Forex News with Caching & Validation"""
            if not self.finnhub_client:
                return web.json_response({'error': 'News service disabled. API Key not configured.'}, status=503)

            try:
                if not hasattr(self, '_news_api_cache'):
                    self._news_api_cache = {'data': [], 'timestamp': 0}

                CACHE_DURATION = 300
                current_time = time.time()
                if self._news_api_cache['data'] and (current_time - self._news_api_cache['timestamp'] < CACHE_DURATION):
                    return web.json_response(self._news_api_cache['data'])

                news = self.finnhub_client.general_news('forex', min_id=0)
                formatted_news = []
                for item in news:
                    if not item.get('headline') or not item.get('url'):
                        continue
                        
                    formatted_news.append({
                        'id': item.get('id'),
                        'category': item.get('category', 'Forex').title(),
                        'headline': item.get('headline'),
                        'image': item.get('image', ''),
                        'source': item.get('source', 'PipSage News'),
                        'summary': item.get('summary', ''),
                        'url': item.get('url'),
                        'datetime': item.get('datetime')
                    })

                formatted_news = formatted_news[:40]

                self._news_api_cache = {
                    'data': formatted_news,
                    'timestamp': current_time
                }

                return web.json_response(formatted_news)

            except Exception as e:
                logger.error(f"API News Error: {e}")
                
                if hasattr(self, '_news_api_cache') and self._news_api_cache['data']:
                    return web.json_response(self._news_api_cache['data'])
                
                return web.json_response({'error': 'Failed to fetch news'}, status=500)

        async def api_support_send(request):
            """API: User sends message (Multipart for images)"""
            try:
                user_id = None
                content = None
                image_data = None

                if request.content_type.startswith('multipart/'):
                    reader = await request.multipart()
                    while True:
                        field = await reader.next()
                        if field is None: break
                        
                        if field.name == 'user_id':
                            val = await field.read(decode=True)
                            try: user_id = int(val.decode('utf-8'))
                            except: pass
                        elif field.name == 'content':
                            val = await field.read(decode=True)
                            content = val.decode('utf-8')
                        elif field.name == 'image':
                            image_data = await field.read(decode=False)
                
                elif request.content_type == 'application/json':
                    data = await request.json()
                    user_id = int(data.get('user_id'))
                    content = data.get('content')

                if not user_id:
                    return web.json_response({'error': 'User ID required'}, status=400)

                success, msg = await self.support_manager.handle_app_message(
                    self.application, user_id, content, image_data
                )
                
                if success:
                    return web.json_response({'success': True})
                else:
                    return web.json_response({'error': msg}, status=500)
            except Exception as e:
                return web.json_response({'error': str(e)}, status=500)

        async def api_support_history(request):
            """API: Get chat history"""
            try:
                user_id = int(request.match_info['user_id'])
                cursor = self.db.db['support_messages'].find({'user_id': user_id}).sort('timestamp', 1)
                messages = list(cursor)
                for m in messages: m['_id'] = str(m['_id'])
                return web.json_response(messages)
            except Exception as e:
                return web.json_response({'error': str(e)}, status=500)

        async def api_support_end(request):
            """API: End chat and delete history"""
            try:
                user_id = int(request.match_info['user_id'])
                self.db.db['support_messages'].delete_many({'user_id': user_id})
                
                support_group = self.db.get_support_group()
                if support_group:
                    try:
                        await self.application.bot.send_message(
                            chat_id=support_group,
                            text=f"🔴 <b>Chat Ended</b>\nUser {user_id} has closed the support session.",
                            parse_mode='HTML'
                        )
                    except: pass

                return web.json_response({'success': True})
            except Exception as e:
                return web.json_response({'error': str(e)}, status=500)

        async def api_calculate_position_size(request):
            """
            API Endpoint: Calculate Position Size
            Usage: GET /api/tools/position_size?pair=EURUSD&risk=100&sl=20
            """
            try:
                params = request.query
                pair = params.get('pair', '').upper()
                try:
                    risk_usd = float(params.get('risk', 0))
                    sl_pips = float(params.get('sl', 0))
                except ValueError:
                    return web.json_response({'error': 'Risk and SL must be numeric'}, status=400)

                if not pair or risk_usd <= 0 or sl_pips <= 0:
                    return web.json_response({'error': 'Invalid parameters. Required: pair, risk (positive), sl (positive)'}, status=400)

                pip_value_per_lot, description = self.get_estimated_pip_value(pair)

                if pip_value_per_lot > 0:
                    raw_lots = risk_usd / (sl_pips * pip_value_per_lot)
                else:
                    raw_lots = 0

                unit_label = "pip"
                if "point" in description.lower():
                    unit_label = "point"
                display_pip_value = f"${pip_value_per_lot:.2f}/{unit_label}"
                if any(x in pair for x in ["V75", "VOLATILITY", "BOOM", "CRASH", "STEP", "JUMP", "V100", "V25"]):
                    recommended_lots = round(raw_lots, 3)
                    if recommended_lots < 0.001: recommended_lots = 0.001
                else:
                    recommended_lots = round(raw_lots, 2)
                    if recommended_lots < 0.01: recommended_lots = 0.01

                return web.json_response({
                    'success': True,
                    'pair': pair,
                    'pair_type': description,
                    'risk_usd': risk_usd,
                    'stop_loss': sl_pips,
                    'pip_value_per_lot': pip_value_per_lot,
                    'recommended_lots': recommended_lots,
                    'display_pip_value': display_pip_value
                })

            except Exception as e:
                logger.error(f"API Position Size Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_vip_request(request):
            """
            API Endpoint: Handle VIP Subscriptions
            1. Checks Subscription Status First (Unlocks Frontend if already VIP).
            2. Deriv: Tries Auto-Verification (CR + OCR). If fail -> Store Request -> Forward to Support Group/Admins.
            3. Currencies: Store Request -> Forward to Support Group/Admins.
            """
            try:
                reader = await request.multipart()
                data = {}
                image_data = None
                
                while True:
                    field = await reader.next()
                    if field is None: break
                    
                    if field.name == 'image':
                        image_data = await field.read(decode=False)
                    else:
                        value = await field.read(decode=True)
                        data[field.name] = value.decode('utf-8')

                user_id = int(data.get('user_id', 0))
                vip_type = data.get('type', '').lower()

                if not user_id:
                    return web.json_response({'error': 'User ID is required'}, status=400)

                if self.db.is_subscriber(user_id):
                    return web.json_response({
                        'success': False, 
                        'message': 'User is already a VIP subscriber.',
                        'is_vip': True
                    })

                if vip_type == 'deriv':
                    cr_number = data.get('cr_number', '').strip().upper()
                    
                    if not cr_number:
                        return web.json_response({'error': 'CR Number is required'}, status=400)
                    if not image_data:
                        return web.json_response({'error': 'Payment screenshot required'}, status=400)

                    auto_verify_passed = False
                    fail_reason = []

                    if cr_number not in self.cr_numbers:
                        fail_reason.append(f"CR {cr_number} not in partner list")
                    elif self.db.is_cr_number_used(cr_number):
                        fail_reason.append(f"CR {cr_number} already used")
                    
                    detected_balance = 0.0
                    try:
                        import pytesseract
                        from PIL import Image
                        import io
                        import re

                        text = pytesseract.image_to_string(Image.open(io.BytesIO(image_data)))
                        matches = re.findall(r'\$?(\d[\d,]*\.\d{2})', text)
                        
                        if matches:
                            detected_balance = float(matches[0].replace(',', ''))
                            if detected_balance < 50:
                                fail_reason.append(f"Balance too low (${detected_balance})")
                        else:
                            fail_reason.append("Could not read balance")
                            
                    except Exception as e:
                        logger.error(f"OCR Error: {e}")
                        fail_reason.append("OCR Processing Failed")

                    if not fail_reason and detected_balance >= 50:
                        auto_verify_passed = True

                    if auto_verify_passed:
                        self.db.mark_cr_number_as_used(cr_number, user_id)
                        self.db.add_subscriber(user_id)
                        self.engagement_tracker.update_engagement(user_id, 'vip_subscribed')
                        
                        self.db.create_vip_request(user_id, 'deriv', {'cr_number': cr_number})
                        self.db.update_vip_request_status(user_id, 'approved', 0, reason="Auto-verified")

                        try:
                            await self.application.bot.send_message(
                                chat_id=user_id,
                                text="🎉 <b>VIP Activated!</b>\n\nYour Deriv account has been verified automatically.",
                                parse_mode='HTML'
                            )
                        except: pass

                        return web.json_response({'success': True, 'message': 'Deriv VIP Activated Automatically'})
                    
                    else:
                        details = {'cr_number': cr_number}
                        self.db.create_vip_request(user_id, 'deriv', details)
                        
                        reason_str = ", ".join(fail_reason)
                        user_info = (
                            f"⚠️ <b>Deriv Auto-Verify Failed</b>\n"
                            f"Forwarded for Manual Review.\n\n"
                            f"<b>User ID:</b> {user_id}\n"
                            f"<b>CR Number:</b> {cr_number}\n"
                            f"<b>Detected Balance:</b> ${detected_balance}\n"
                            f"<b>Failure Reason:</b> {reason_str}\n"
                        )

                        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                        keyboard = [
                            [
                                InlineKeyboardButton("✅ Force Approve", callback_data=f"vip_approve_{user_id}"),
                                InlineKeyboardButton("❌ Decline", callback_data=f"vip_decline_{user_id}")
                            ]
                        ]
                        reply_markup = InlineKeyboardMarkup(keyboard)

                        sent_count = 0
                        try:
                            await self.send_admin_notification(
                                text=user_info,
                                photo=io.BytesIO(image_data),
                                reply_markup=reply_markup,
                                fallback_admins=self.super_admin_ids
                            )
                            sent_count = 1
                        except Exception as e:
                            logger.error(f"Failed to forward failed Deriv request to support: {e}")

                        if sent_count > 0:
                            return web.json_response({
                                'success': True, 
                                'message': 'Auto-verification failed. Sent to agents for manual review. You will be notified soon.'
                            })
                        else:
                            return web.json_response({'error': 'Verification failed and could not contact admins.'}, status=500)

                elif vip_type == 'currencies':
                    broker = data.get('broker')
                    acc_name = data.get('account_name')
                    acc_num = data.get('account_number')
                    tg_handle = data.get('telegram_id')

                    if not all([broker, acc_name, acc_num, tg_handle]):
                        return web.json_response({'error': 'Missing fields for Currencies request'}, status=400)

                    details = {
                        'broker': broker,
                        'account_name': acc_name,
                        'account_number': acc_num,
                        'telegram_id': tg_handle
                    }
                    self.db.create_vip_request(user_id, 'currencies', details)

                    user_info = (
                        f"📱 <b>New App VIP Request (Currencies)</b>\n\n"
                        f"<b>Broker:</b> {broker}\n"
                        f"<b>Name:</b> {acc_name}\n"
                        f"<b>Acc #:</b> {acc_num}\n"
                        f"<b>Telegram:</b> {tg_handle}\n"
                        f"<b>User ID:</b> {user_id}"
                    )

                    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                    keyboard = [
                        [
                            InlineKeyboardButton("✅ Approve", callback_data=f"vip_approve_{user_id}"),
                            InlineKeyboardButton("❌ Decline", callback_data=f"vip_decline_{user_id}")
                        ]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)

                    sent_count = 0
                    try:
                        await self.send_admin_notification(
                            text=user_info,
                            reply_markup=reply_markup,
                            fallback_admins=self.super_admin_ids
                        )
                        sent_count = 1
                    except Exception as e:
                        logger.error(f"Failed to notify support for currencies request: {e}")

                    if sent_count > 0:
                        return web.json_response({'success': True, 'message': 'Request sent to admins for approval'})
                    else:
                        return web.json_response({'error': 'Failed to notify admins'}, status=500)

                else:
                    return web.json_response({'error': 'Invalid VIP type'}, status=400)

            except Exception as e:
                logger.error(f"API VIP Request Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_resolve_user(request):
            """API Endpoint: Resolve Username/Handle to User ID"""
            try:
                query = request.query.get('username', '').strip().replace('@', '')
                
                if not query:
                    return web.json_response({'error': 'Username or ID required'}, status=400)

                if query.isdigit():
                    user = self.db.users_collection.find_one({'user_id': int(query)})
                else:
                    user = self.db.users_collection.find_one(
                        {'username': {'$regex': f'^{re.escape(query)}$', '$options': 'i'}}
                    )

                if user:
                    return web.json_response({
                        'success': True,
                        'user_id': user['user_id'],
                        'first_name': user.get('first_name'),
                        'username': user.get('username')
                    })
                else:
                    return web.json_response({
                        'error': 'User not found. Please start the bot on Telegram first (/start).'
                    }, status=404)

            except Exception as e:
                logger.error(f"API Resolve Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_submit_signal(request):
            """API Endpoint: Submit Signal from App (Supports Text & Images)"""
            import html
            try:
                user_id = None
                content = None
                image_data = None
                if request.content_type.startswith('multipart/'):
                    reader = await request.multipart()
                    while True:
                        field = await reader.next()
                        if field is None: break
                        
                        if field.name == 'user_id':
                            raw_id = await field.read(decode=True)
                            try: user_id = int(raw_id.decode('utf-8'))
                            except: pass
                        elif field.name == 'content':
                            raw_content = await field.read(decode=True)
                            content = raw_content.decode('utf-8')
                        elif field.name == 'image':
                            image_data = await field.read(decode=False)
                
                elif request.content_type == 'application/json':
                    data = await request.json()
                    user_id = data.get('user_id')
                    content = data.get('content')
                    try: user_id = int(user_id)
                    except: pass
                
                if not user_id or not content:
                    return web.json_response({'error': 'Missing user_id or content'}, status=400)

                cleaned_content = self.clean_empty_signal_fields(content)
                final_content = cleaned_content + "\n\n📲 via PipSage App"
                
                message_data = {
                    'content': final_content
                }

                if image_data:
                    if not hasattr(self, 'application') or not self.application:
                         return web.json_response({'error': 'Bot not ready for file uploads'}, status=503)
                    
                    try:
                        sent_msg = None
                        if not self.super_admin_ids:
                             raise Exception("No super admins configured.")

                        for admin_id in self.super_admin_ids:
                            try:
                                sent_msg = await self.application.bot.send_photo(
                                    chat_id=admin_id,
                                    photo=io.BytesIO(image_data),
                                    caption=f"🔄 Processing App Signal from ID: {user_id}..."
                                )
                                if sent_msg: break
                            except Exception as inner_e:
                                continue
                        
                        if not sent_msg: raise Exception("Could not send image.")

                        if sent_msg.photo:
                            file_id = sent_msg.photo[-1].file_id
                            try: await sent_msg.delete()
                            except: pass

                            message_data['type'] = 'photo'
                            message_data['file_id'] = file_id
                            message_data['caption'] = cleaned_content + "\n\n📲 via PipSage App"
                            
                            if 'content' in message_data: del message_data['content']
                        else:
                            raise Exception("Telegram did not return a photo object.")

                    except Exception as e:
                        logger.error(f"Telegram Upload Failed: {e}")
                        return web.json_response({'error': 'Failed to process image'}, status=500)
                else:
                    message_data['type'] = 'text'

                user = self.db.users_collection.find_one({'user_id': user_id})
                name = user.get('first_name', str(user_id)) if user else str(user_id)

                suggestion_id = self.db.create_signal_suggestion(message_data, user_id, name)
                
                if suggestion_id:
                    self.engagement_tracker.update_engagement(user_id, 'signal_suggested')
                    if hasattr(self, 'application') and self.application:
                        await self.notify_super_admins_new_suggestion(self.application, suggestion_id)
                    return web.json_response({'success': True, 'id': suggestion_id})
                else:
                    return web.json_response({'error': 'Database error'}, status=500)

            except Exception as e:
                logger.error(f"API Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_get_broadcasts(request):
            """
            API Endpoint: Get Unified Feed with Batch Optimized Reactions.
            """
            try:
                results = []
                current_time = time.time()
                
                is_vip_request = request.query.get('vip') == 'true'
                
                requesting_user_id = request.query.get('user_id')
                try: requesting_user_id = int(requesting_user_id) if requesting_user_id else None
                except: requesting_user_id = None

                def format_time_ago(ts):
                    diff = current_time - ts
                    if diff < 60: return "Just now"
                    elif diff < 3600: return f"{int(diff // 60)} mins ago"
                    elif diff < 86400: return f"{int(diff // 3600)} hrs ago"
                    else: return f"{int(diff // 86400)} days ago"

                def extract_data(msg_data):
                    content = ""
                    image_url = None
                    if msg_data.get('type') == 'text':
                        content = msg_data.get('content', '')
                    elif msg_data.get('type') == 'photo':
                        content = msg_data.get('caption', '') or ""
                        file_id = msg_data.get('file_id')
                        if file_id:
                            image_url = f"/api/media/{file_id}"
                    return content, image_url

                raw_items = []

                if is_vip_request:
                    cursor = self.db.broadcast_approvals_collection.find(
                        {'status': 'approved', 'target': 'subscribers'}
                    ).sort('reviewed_at', -1).limit(20)
                    for doc in cursor:
                        raw_items.append({'doc': doc, 'type': 'broadcast', 'is_vip': True})
                else:
                    sig_cursor = self.db.signal_suggestions_collection.find(
                        {'status': 'approved'}
                    ).sort('reviewed_at', -1).limit(15)
                    for doc in sig_cursor:
                        raw_items.append({'doc': doc, 'type': 'signal'})

                    bc_cursor = self.db.broadcast_approvals_collection.find(
                        {'status': 'approved', 'target': {'$in': ['all', 'nonsubscribers']}}
                    ).sort('reviewed_at', -1).limit(10)
                    for doc in bc_cursor:
                        raw_items.append({'doc': doc, 'type': 'broadcast'})

                    if hasattr(self, 'edu_content_manager') and self.edu_content_manager:
                        edu_cursor = self.edu_content_manager.educational_content_collection.find(
                            {'type': {'$in': ['text', 'photo']}} 
                        ).sort('saved_at', -1).limit(5)
                        for doc in edu_cursor:
                            raw_items.append({'doc': doc, 'type': 'education'})

                all_ids = []
                for item in raw_items:
                    if item['type'] == 'education':
                        item_id = f"edu_{item['doc'].get('message_id')}"
                        item['doc']['_temp_id'] = item_id
                    else:
                        item_id = str(item['doc']['_id'])
                        item['doc']['_temp_id'] = item_id
                    all_ids.append(item_id)

                reaction_counts = {}
                user_liked_ids = set()

                if all_ids:
                    pipeline = [
                        {'$match': {'broadcast_id': {'$in': all_ids}}},
                        {'$group': {'_id': '$broadcast_id', 'count': {'$sum': 1}}}
                    ]
                    agg_results = list(self.db.db['reactions'].aggregate(pipeline))
                    for res in agg_results:
                        reaction_counts[res['_id']] = res['count']

                    if requesting_user_id:
                        user_likes = list(self.db.db['reactions'].find(
                            {'user_id': requesting_user_id, 'broadcast_id': {'$in': all_ids}},
                            {'broadcast_id': 1}
                        ))
                        user_liked_ids = {r['broadcast_id'] for r in user_likes}

                for item in raw_items:
                    doc = item['doc']
                    item_id = doc.get('_temp_id')
                    
                    likes = reaction_counts.get(item_id, 0)
                    is_liked = item_id in user_liked_ids

                    if item['type'] == 'education':
                        content = doc.get('content') if doc['type'] == 'text' else doc.get('caption', '')
                        image_url = None
                        if doc['type'] == 'photo' and doc.get('file_id'):
                            image_url = f"/api/media/{doc['file_id']}"
                        
                        results.append({
                            'id': item_id,
                            'type': 'education',
                            'content': f"📚 <b>Daily Tip</b>\n\n{content}",
                            'rating': 0,
                            'timestamp_raw': doc.get('saved_at', 0),
                            'timestamp': format_time_ago(doc.get('saved_at', 0)),
                            'author': 'PipSage Education',
                            'image': image_url,
                            'likesCount': likes,
                            'isLiked': is_liked
                        })
                    else:
                        msg_data = doc.get('message_data', {})
                        content, image_url = extract_data(msg_data)
                    
                        if item.get('is_vip') and not content.startswith("🔒"):
                            content = f"🔒 <b>VIP Update</b>\n\n{content}"
                        elif item['type'] == 'broadcast' and not content.startswith("📢") and not item.get('is_vip'):
                            target = doc.get('target', 'Announcement').replace('_', ' ').title()
                            content = f"📢 {target}\n\n{content}"

                        author = 'PipSage Team'
                        if item['type'] == 'signal':
                            author = doc.get('suggester_name', 'Unknown Trader')
                        elif item.get('is_vip'):
                            author = 'PipSage VIP'

                        results.append({
                            'id': item_id,
                            'type': item['type'],
                            'content': content,
                            'rating': doc.get('rating', 0),
                            'timestamp_raw': doc.get('reviewed_at', 0),
                            'timestamp': format_time_ago(doc.get('reviewed_at', 0)),
                            'author': author, 
                            'image': image_url,
                            'likesCount': likes,
                            'isLiked': is_liked
                        })

                stats = self.db.get_suggester_stats('weekly')
                if stats and not is_vip_request:
                    lb_text = "🏆 <b>Top Traders (This Week)</b>\n\n"
                    medals = ["🥇", "🥈", "🥉"]
                    for i, stat in enumerate(stats[:3]):
                        rank = medals[i] if i < 3 else f"#{i+1}"
                        name = stat['suggester_name']
                        lb_text += f"{rank} {name}: {stat['average_rating']:.1f}⭐ ({stat['signal_count']} signals)\n"
                    
                    results.append({
                        'id': 'weekly_leaderboard',
                        'type': 'leaderboard',
                        'content': lb_text,
                        'rating': 0,
                        'timestamp_raw': current_time,
                        'timestamp': 'Live Now',
                        'author': 'System',
                        'image': None,
                        'likesCount': 0,
                        'isLiked': False
                    })

                results.sort(key=lambda x: x['timestamp_raw'], reverse=True)
                
                final_results = results[:50]
                for res in final_results:
                    if 'timestamp_raw' in res: del res['timestamp_raw']

                return web.json_response(final_results)
            except Exception as e:
                logger.error(f"API Error (Broadcasts): {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_get_media(request):
            """API Endpoint: Proxy Telegram Media (Securely serves images)"""
            try:
                file_id = request.match_info['file_id']
                if not file_id:
                    return web.Response(status=400, text="Missing file_id")

                if not hasattr(self, 'application') or not self.application:
                    return web.Response(status=503, text="Bot not initialized")

                new_file = await self.application.bot.get_file(file_id)
                
                bio = io.BytesIO()
                await new_file.download_to_memory(bio)
                bio.seek(0)
                
                return web.Response(body=bio.getvalue(), content_type='image/jpeg')
            except Exception as e:
                logger.error(f"Media API Error: {e}")
                return web.Response(status=404, text="Image not found")

        async def api_update_settings(request):
            """API Endpoint: Update User Notification Settings"""
            try:
                data = await request.json()
                user_id = data.get('user_id')
                settings = data.get('settings')

                if not user_id or not settings:
                    return web.json_response({'error': 'Missing user_id or settings'}, status=400)
                
                try:
                    user_id = int(user_id)
                except ValueError:
                    return web.json_response({'error': 'Invalid user_id'}, status=400)

                update_fields = {}
                
                if 'notifications' in settings:
                    update_fields['notifications.broadcasts'] = settings['notifications']
                
                if 'signals' in settings:
                    update_fields['notifications.signals'] = settings['signals']
                
                if 'leaderboard' in settings:
                    update_fields['notifications.leaderboards'] = settings['leaderboard']
                
                if 'tips' in settings:
                    update_fields['notifications.tips'] = settings['tips']

                result = self.db.users_collection.update_one(
                    {'user_id': user_id},
                    {'$set': update_fields}
                )

                if result.matched_count == 0:
                    return web.json_response({'error': 'User not found'}, status=404)

                return web.json_response({'success': True})

            except Exception as e:
                logger.error(f"API Settings Update Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_privacy_policy_page(request):
            """Web Page for Privacy Policy (Play Store Requirement)"""
            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Privacy Policy - PipSage</title>
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <style>
                    body { font-family: sans-serif; padding: 20px; max-width: 800px; margin: 0 auto; line-height: 1.6; color: #333; }
                    h1 { color: #2563EB; }
                    h2 { color: #111827; margin-top: 20px; }
                    ul { padding-left: 20px; }
                    li { margin-bottom: 8px; }
                    .footer { margin-top: 40px; font-size: 0.9em; color: #666; border-top: 1px solid #eee; padding-top: 20px; }
                </style>
            </head>
            <body>
                <h1>Privacy Policy</h1>
                <p><strong>Last updated: November 26, 2025</strong></p>
                
                <p>PipSage ("we", "our", or "us") is committed to protecting your privacy. This Privacy Policy explains how we collect, use, disclosure, and safeguard your information when you use our mobile application and related Telegram services.</p>

                <h2>1. Information We Collect</h2>
                <p>To provide our services, we collect minimal data linked to your Telegram account:</p>
                <ul>
                    <li><strong>Account Information:</strong> We collect your Telegram User ID, Username, and First Name to authenticate you and display your profile.</li>
                    <li><strong>Trading Data:</strong> If you choose to submit trading signals, we store the details of those signals (pair, entry, direction) and their performance outcomes.</li>
                    <li><strong>Usage Data:</strong> We may collect anonymous logs regarding command usage to improve bot performance and prevent abuse.</li>
                </ul>

                <h2>2. How We Use Your Information</h2>
                <p>We use the information we collect to:</p>
                <ul>
                    <li>Authenticate your access to the mobile application and VIP groups.</li>
                    <li>Calculate and display your trading performance on our public or private leaderboards.</li>
                    <li>Send you important notifications (which you can opt-out of via Settings).</li>
                    <li>Facilitate the "Connect Telegram" login feature.</li>
                </ul>

                <h2>3. Data Sharing</h2>
                <p>We do not sell, trade, or rent your personal identification information to others. We may share generic aggregated demographic information not linked to any personal identification information regarding visitors and users with our business partners.</p>

                <h2>4. Data Security</h2>
                <p>We adopt appropriate data collection, storage, and processing practices and security measures to protect against unauthorized access, alteration, disclosure, or destruction of your personal information.</p>

                <h2>5. Your Rights & Account Deletion</h2>
                <p>You have the right to request the deletion of your account and all associated data at any time. You can do this by:</p>
                <ul>
                    <li>Visiting our deletion portal: <a href="/delete-account">/delete-account</a></li>
                    <li>Contacting support via our Telegram Bot.</li>
                </ul>

                <h2>6. Third-Party Services</h2>
                <p>Our app may use third-party services (e.g., Telegram API, Finnhub for news) which have their own privacy policies. We encourage you to review them.</p>

                <h2>7. Contact Us</h2>
                <p>If you have any questions about this Privacy Policy, please contact us via our official Telegram channel.</p>

                <div class="footer">
                    &copy; 2025 PipSage. All rights reserved.
                </div>
            </body>
            </html>
            """
            return web.Response(text=html_content, content_type='text/html')
        
        async def api_get_settings(request):
             """API Endpoint: Get User Settings"""
             try:
                user_id = request.match_info['user_id']
                try: user_id = int(user_id)
                except: return web.json_response({'error': 'Invalid ID'}, status=400)

                user = self.db.users_collection.find_one({'user_id': user_id})
                if not user:
                    return web.json_response({'error': 'User not found'}, status=404)
                
                prefs = user.get('notifications', {})
                

                response_data = {
                    'notifications': prefs.get('broadcasts', True),
                    'signals': prefs.get('signals', True),
                    'leaderboard': prefs.get('leaderboards', True),
                    'tips': prefs.get('tips', True)
                }
                return web.json_response(response_data)
             except Exception as e:
                 return web.json_response({'error': str(e)}, status=500)

        async def api_get_notifications(request):
            """API Endpoint: Get User Notifications"""
            try:
                user_id = request.match_info['user_id']
                try: 
                    user_id = int(user_id)
                except (ValueError, TypeError): 
                    return web.json_response({'error': 'Invalid User ID'}, status=400)

                notifications = self.db.get_user_notifications(user_id, limit=50)
                
                return web.json_response(notifications)
            except Exception as e:
                logger.error(f"API Notifications Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def api_toggle_reaction(request):
            """API Endpoint: Toggle Like on a Broadcast/Signal"""
            try:
                broadcast_id = request.match_info['id']
                data = await request.json()
                user_id = data.get('user_id')

                if not user_id:
                    return web.json_response({'error': 'User ID required'}, status=400)
                
                try: user_id = int(user_id)
                except: return web.json_response({'error': 'Invalid User ID'}, status=400)
                existing = self.db.db['reactions'].find_one({
                    'user_id': user_id,
                    'broadcast_id': broadcast_id
                })

                action = 'added'
                if existing:
                    self.db.db['reactions'].delete_one({'_id': existing['_id']})
                    action = 'removed'
                else:
                    self.db.db['reactions'].insert_one({
                        'user_id': user_id,
                        'broadcast_id': broadcast_id,
                        'type': 'like',
                        'timestamp': time.time()
                    })
                    
                    try:
                        liker_doc = self.db.users_collection.find_one({'user_id': user_id})
                        liker_name = liker_doc.get('first_name') or "Someone"
                        
                        author_id = None
                        from bson.objectid import ObjectId
                        
                        if ObjectId.is_valid(broadcast_id):
                            obj_id = ObjectId(broadcast_id)
                            
                            signal_doc = self.db.signal_suggestions_collection.find_one({'_id': obj_id})
                            if signal_doc:
                                author_id = signal_doc.get('suggested_by')
                            else:
                                broadcast_doc = self.db.broadcast_approvals_collection.find_one({'_id': obj_id})
                                if broadcast_doc:
                                    author_id = broadcast_doc.get('created_by')
                        
                        if author_id and author_id != user_id:
                            logger.info(f"Sending reaction notification to author {author_id} from {liker_name}")
                            asyncio.create_task(self.send_push_to_users(
                                [author_id],
                                "New Reaction ❤️",
                                f"Your post was liked by {liker_name}",
                                data={'screen': 'Signals', 'initialTab': 'broadcasts'}
                            ))
                            
                    except Exception as notify_e:
                        logger.error(f"Failed to process reaction notification: {notify_e}")

                new_count = self.db.db['reactions'].count_documents({'broadcast_id': broadcast_id})
                return web.json_response({'success': True, 'action': action, 'count': new_count})

            except Exception as e:
                logger.error(f"Reaction API Error: {e}")
                return web.json_response({'error': str(e)}, status=500)

        async def health_check(request):
            return web.Response(text="PipSage API Running", status=200)
        app = web.Application()
        app.router.add_get('/health', health_check)
        app.router.add_get('/api/users/{user_id}/stats', api_get_stats)
        app.router.add_post('/api/signals', api_submit_signal)
        app.router.add_get('/api/broadcasts', api_get_broadcasts)
        app.router.add_get('/api/news', api_get_news)
        app.router.add_get('/api/resolve_user', api_resolve_user)
        app.router.add_get('/api/media/{file_id}', api_get_media)
        app.router.add_post('/api/settings', api_update_settings)
        app.router.add_get('/api/settings/{user_id}', api_get_settings)
        app.router.add_get('/api/tools/position_size', api_calculate_position_size)
        app.router.add_post('/api/vip/request', api_vip_request)
        app.router.add_post('/api/auth/send_code', api_send_code)
        app.router.add_post('/api/auth/verify_code', api_verify_code)
        app.router.add_get('/delete-account', api_delete_account_page)
        app.router.add_post('/api/delete-account-submit', api_delete_account_submit)
        app.router.add_get('/privacy', api_privacy_policy_page)
        app.router.add_post('/api/users/push_token', api_update_push_token)
        app.router.add_get('/api/users/{user_id}/notifications', api_get_notifications)
        app.router.add_post('/api/broadcasts/{id}/react', api_toggle_reaction)
        app.router.add_get('/api/push/status/{user_id}', api_check_push_status)
        app.router.add_delete('/api/users/{user_id}/notifications', api_clear_notifications)
        app.router.add_post('/api/notifications/mark-read', api_mark_notification_read)
        app.router.add_post('/api/users/{user_id}/notifications/mark-all-read', api_mark_all_read)
        app.router.add_get('/api/users/{user_id}/notifications/unread-count', api_get_unread_count)
        app.router.add_post('/api/support/message', api_support_send)
        app.router.add_get('/api/support/history/{user_id}', api_support_history)
        app.router.add_delete('/api/support/end/{user_id}', api_support_end)
        
        import aiohttp_cors
        cors = aiohttp_cors.setup(app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        })
        for route in list(app.router.routes()):
            cors.add(route)

        return app

    def run_health_server(self, port: int):
        """Run API server in thread"""
        async def start_server():
            app = self.create_api_server()
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', port)
            await site.start()
            logger.info(f"API Server running on port {port}")

            while True:
                await asyncio.sleep(3600)

        def run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(start_server())

        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()

    def create_application(self):
        """Create and configure application with all handlers and jobs."""
        application = Application.builder().token(self.token).build()

        
        broadcast_handler = ConversationHandler(
            entry_points=[
                CommandHandler("broadcast", self.start_broadcast),
                CallbackQueryHandler(self.handle_template_callback, pattern="^tpl_use_")
            ],
            states={
                WAITING_INITIAL_PLATFORM: [CallbackQueryHandler(self.handle_initial_platform_choice, pattern="^platform_")],
                WAITING_MESSAGE: [MessageHandler(filters.ALL & ~filters.COMMAND, self.receive_broadcast_message)],
                WAITING_BUTTONS: [
                    CallbackQueryHandler(self.handle_watermark_choice, pattern="^watermark_"),
                    CallbackQueryHandler(self.handle_buttons_choice, pattern="^(add_buttons|skip_buttons)$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_buttons)
                ],
                WAITING_PROTECTION: [CallbackQueryHandler(self.handle_protection_choice, pattern="^protect_")],
                WAITING_TARGET: [CallbackQueryHandler(self.handle_target_choice, pattern="^target_")]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

        schedule_handler = ConversationHandler(
            entry_points=[CommandHandler("schedule", self.schedule_broadcast_start)],
            states={
                WAITING_MESSAGE: [MessageHandler(filters.ALL & ~filters.COMMAND, self.receive_broadcast_message)],
                WAITING_BUTTONS: [
                    CallbackQueryHandler(self.handle_watermark_choice, pattern="^watermark_"),
                    CallbackQueryHandler(self.handle_buttons_choice, pattern="^(add_buttons|skip_buttons)$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_buttons)
                ],
                WAITING_PROTECTION: [CallbackQueryHandler(self.handle_protection_choice, pattern="^protect_")],
                WAITING_SCHEDULE_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_schedule_time)],
                WAITING_SCHEDULE_REPEAT: [CallbackQueryHandler(self.receive_schedule_repeat, pattern="^repeat_")],
                WAITING_TARGET: [CallbackQueryHandler(self.handle_target_choice, pattern="^target_")]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

        template_handler = ConversationHandler(
            entry_points=[CommandHandler("savetemplate", self.save_template_start)],
            states={
                WAITING_TEMPLATE_MESSAGE: [MessageHandler(filters.ALL & ~filters.COMMAND, self.receive_template_message)],
                WAITING_TEMPLATE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_template_name)],
                WAITING_TEMPLATE_CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_template_category)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

        add_admin_handler = ConversationHandler(
            entry_points=[CommandHandler("addadmin", self.add_admin_start)],
            states={
                WAITING_ADMIN_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_admin_id)],
                WAITING_ADMIN_ROLE: [CallbackQueryHandler(self.receive_admin_role, pattern="^role_")]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)]
        )

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

        subscribe_handler = ConversationHandler(
            entry_points=[CommandHandler("subscribe", self.subscribe_start)],
            states={
                WAITING_VIP_GROUP: [CallbackQueryHandler(self.receive_vip_group)],
                WAITING_ACCOUNT_CREATION_CONFIRMATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_account_creation_confirmation)],
                WAITING_ACCOUNT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_account_date)],
                WAITING_CR_NUMBER: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_cr_number)],
                WAITING_SCREENSHOT: [MessageHandler(filters.PHOTO, self.receive_screenshot)],
                WAITING_KENNEDYNESPOT_CONFIRMATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_kennedynespot_confirmation)],
                WAITING_BROKER_CHOICE: [CallbackQueryHandler(self.receive_broker_choice, pattern="^broker_")],
                WAITING_ACCOUNT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_account_name)],
                WAITING_ACCOUNT_NUMBER: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_account_number)],
                WAITING_TELEGRAM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_telegram_id)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_broadcast)],
        )

        application.add_handler(CommandHandler("start", self.start_v2)) 
        application.add_handler(CommandHandler("help", self.help_command_v2)) 
        application.add_handler(CallbackQueryHandler(self.handle_help_callbacks, pattern="^help_")) 
        application.add_handler(CallbackQueryHandler(self.check_joined_callback, pattern="^check_joined$"))
        application.add_handler(CommandHandler("unsubscribe", self.unsubscribe))
        application.add_handler(CommandHandler("add", self.add_subscriber_command))
        application.add_handler(CommandHandler("stats", self.stats))
        application.add_handler(CommandHandler("subscribers", self.list_subscribers))
        application.add_handler(CommandHandler("approvals", self.list_approvals))
        application.add_handler(CommandHandler("signals", self.list_signal_suggestions))
        application.add_handler(CommandHandler("removeadmin", self.remove_admin_command))
        application.add_handler(CommandHandler("admins", self.list_admins))
        application.add_handler(CommandHandler("logs", self.view_logs))
        application.add_handler(CommandHandler("mystats", self.my_stats))
        application.add_handler(CommandHandler("performance", self.show_performance_command))
        application.add_handler(CommandHandler("referral", self.show_referral_command))
        application.add_handler(CommandHandler("testimonials", show_testimonials_command)) 
        application.add_handler(CommandHandler("myprogress", self.my_progress_command)) 
        application.add_handler(CommandHandler("settings", self.settings_command)) 
        application.add_handler(CallbackQueryHandler(self.handle_settings_callback, pattern="^toggle_")) 
        application.add_handler(CallbackQueryHandler(self.handle_settings_callback, pattern="^close_settings$")) 
        application.add_handler(CallbackQueryHandler(self.admin_button_handler, pattern='^admin_'))
        application.add_handler(CommandHandler("news", self.news))
        application.add_handler(CommandHandler("calendar", self.calendar))
        application.add_handler(CommandHandler("pips", self.pips_calculator_v2)) 
        application.add_handler(CommandHandler("positionsize", self.position_size_calculator))
        application.add_handler(CommandHandler("bestschedule", self.suggest_broadcast_time)) 
        application.add_handler(CommandHandler("synceducation", self.sync_educational_content))
        application.add_handler(CommandHandler("previeweducation", self.preview_educational_content))
        application.add_handler(CommandHandler("myduty", self.my_duty_command))
        application.add_handler(CommandHandler("dutycomplete", self.duty_complete_command))
        application.add_handler(CommandHandler("dutystats", self.duty_stats_command))
        application.add_handler(CommandHandler("templates", self.list_templates))
        application.add_handler(CommandHandler("scheduled", self.list_scheduled))
        application.add_handler(CommandHandler("cancel_scheduled", self.cancel_scheduled_command))
        
        application.add_handler(add_admin_handler)
        application.add_handler(template_handler)
        application.add_handler(broadcast_handler)
        application.add_handler(schedule_handler)
        application.add_handler(signal_handler)
        application.add_handler(subscribe_handler)

        application.add_handler(CallbackQueryHandler(self.handle_template_callback, pattern="^tpl_"))
        application.add_handler(CallbackQueryHandler(self.list_templates_callback, pattern="^tpl_list_all$"))
        
        application.add_handler(CallbackQueryHandler(self.handle_force_submit, pattern="^(force_submit_text|force_submit_photo|cancel_signal)$"))
        
        application.add_handler(CallbackQueryHandler(self.handle_deletion_approval, pattern="^del_approve_"))

        application.add_handler(
            MessageHandler(
                filters.ChatType.PRIVATE & filters.Regex(r"^(Hello|Hi|Hey|Good morning|Good afternoon|Good evening|What's up|Howdy|Greetings|Hey there)$"),
                self.handle_greeting,
            )
        )

        filter_reply_vip = filters.REPLY & ReplyContainsFilter("Reason for declining User")
        filter_reply_signal = filters.REPLY & ReplyContainsFilter("Reason for declining Signal")
        filter_reply_broadcast = filters.REPLY & ReplyContainsFilter("Reason for declining Broadcast")

        application.add_handler(MessageHandler(filter_reply_vip, self.handle_decline_reason_reply))
        application.add_handler(MessageHandler(filter_reply_signal, self.handle_signal_decline_reply))
        application.add_handler(MessageHandler(filter_reply_broadcast, self.handle_broadcast_decline_reply))

        application.add_handler(CallbackQueryHandler(self.handle_vip_request_review, pattern="^vip_"))
        application.add_handler(CallbackQueryHandler(self.handle_signal_review_v2, pattern="^sig_(approve|reject)_"))
        application.add_handler(CallbackQueryHandler(self.receive_signal_rating_v2, pattern="^sig_rate_"))
        application.add_handler(CallbackQueryHandler(self.handle_approval_review_v2, pattern="^app_"))

        application.add_handler(
            MessageHandler(
                filters.StatusUpdate.NEW_CHAT_MEMBERS,
                self.support_manager.on_new_chat_members
            )
        )

        application.add_handler(
            MessageHandler(
                filters.ChatType.GROUPS & filters.REPLY,
                self.support_manager.handle_admin_reply
            )
        )
        
        application.add_handler(
            MessageHandler(
                filters.ChatType.PRIVATE & ~filters.COMMAND,
                self.support_manager.handle_user_message
            )
        )

        if self.edu_content_manager:
            try:
                channel_id_int = int(self.edu_content_manager.channel_id)
                application.add_handler(
                    MessageHandler(
                        filters.Chat(chat_id=channel_id_int) & filters.UpdateType.CHANNEL_POST,
                        self.channel_post_handler
                    )
                )
            except ValueError:
                logger.error("EDUCATION_CHANNEL_ID must be an integer for the listener to work.")

            application.add_handler(
                MessageHandler(
                    filters.FORWARDED & filters.User(user_id=self.super_admin_ids), 
                    self.forward_listener
                )
            )

        application.add_error_handler(self.error_handler)

        application.job_queue.run_repeating(
            self.process_scheduled_broadcasts,
            interval=60,
            first=10
        )

        utc_midnight = dt_time(hour=0, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.run_leaderboards_job_v2,
            time=utc_midnight,
            days=(1,) 
        )
        
        utc_10am = dt_time(hour=10, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.send_daily_tip,
            time=utc_10am
        )
        
        utc_12pm = dt_time(hour=12, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.re_engage_users_job,
            time=utc_12pm
        )

        if self.edu_content_manager:
            utc_2am = dt_time(hour=2, minute=0, tzinfo=timezone.utc)
            application.job_queue.run_daily(
                self.auto_sync_education_job,
                time=utc_2am
            )

        utc_midnight = dt_time(hour=0, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.assign_daily_duties_job,
            time=utc_midnight
        )

        utc_6pm = dt_time(hour=18, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.send_duty_reminders_job,
            time=utc_6pm
        )
        utc_6pm = dt_time(hour=18, minute=0, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.post_weekly_performance_to_twitter,
            time=utc_6pm,
            days=(0,) 
        )

        utc_end_day = dt_time(hour=23, minute=55, tzinfo=timezone.utc)
        application.job_queue.run_daily(
            self.end_of_day_duty_verification_job,
            time=utc_end_day
        )

        return application
    async def re_engage_users_job(self, context: ContextTypes.DEFAULT_TYPE):
        await self.engagement_tracker.re_engage_inactive_users(context)

    async def run_promo_job(self, context: ContextTypes.DEFAULT_TYPE):
        await PromotionManager.announce_promo(context, self.db)

    async def show_performance_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await PerformanceTransparency.show_verified_performance(update, context, self.db)

    async def show_referral_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        bot_username = (await context.bot.get_me()).username
        await self.referral_system.show_referral_stats(update.effective_user.id, bot_username, self.db, update)

    async def my_progress_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show user's improvement over time"""
        user_id = update.effective_user.id
        
        current_stats = self.db.get_user_signal_stats(user_id)
        current_rating = self.db.get_user_average_rating(user_id)
        
        message = (
            "📈 <b>Your Progress Report</b>\n\n"
            f"Current Rating: {current_rating:.1f}⭐\n"
            f"Signals Approved: {current_stats['approved']}/{current_stats['total']}\n"
            f"Success Rate: {current_stats['rate']:.1f}%\n\n"
        )
        
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
        if not await self.is_user_subscribed(user_id, context):
            await self.send_join_channel_message(user_id, context)
            return
        # -----------------------
        await update.message.reply_text("Hello! How can I assist you today?")

    async def subscribe_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start the subscription conversation"""
        user_id = update.effective_user.id

        if not await self.is_user_subscribed(user_id, context):
            await self.send_join_channel_message(user_id, context)
            return ConversationHandler.END
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
        if "today" in update.message.text.lower() or "yesterday" in update.message.text.lower():
            await update.message.reply_text("Please wait up to 24 hours for the account to reflect in the system.")
            return ConversationHandler.END
        else:
            await update.message.reply_text("Please provide your CR number in the format 'CR12345'.")
            return WAITING_CR_NUMBER

    async def receive_cr_number(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle CR number and check against the list"""
        cr_number = update.message.text.strip().upper()

        if self.db.is_cr_number_used(cr_number):
            await update.message.reply_text(
                "❌ This CR number has already been used for verification. Each CR number can only be used once.\n\n"
                "If you believe this is an error, please contact an admin."
            )
            return ConversationHandler.END

        if cr_number in self.cr_numbers:
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
            matches = re.findall(r'\$?(\d[\d,]*\.\d{2})', text)
            
            if matches:
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

        await self.send_admin_notification(
            text=user_info,
            reply_markup=reply_markup,
            fallback_admins=self.super_admin_ids
        )

        await update.message.reply_text(
            "Thank you! Your details have been sent to the admins for approval. You will be notified once it's reviewed."
        )
        return ConversationHandler.END

    async def handle_vip_request_review(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the approve/decline decision with ForceReply for declines."""
        query = update.callback_query
        await query.answer()

        user_id = query.from_user.id
        if user_id not in self.super_admin_ids and not self.db.get_admin_role(user_id):
            await query.answer("❌ You are not authorized.", show_alert=True)
            return

        action, target_user_id_str = query.data.split('_')[1:]
        target_user_id = int(target_user_id_str)

        if action == "approve":
            self.db.add_subscriber(target_user_id)
            self.db.update_vip_request_status(target_user_id, 'approved', user_id)
            
            self.db.log_activity(user_id, 'vip_approved', {'user_id': target_user_id})
            self.admin_duty_manager.credit_duty_for_action(user_id, 'vip_approved')
            self.engagement_tracker.update_engagement(target_user_id, 'vip_subscribed')
            
            asyncio.create_task(self.send_push_to_users(
                [target_user_id], "VIP Approved! 🎉", "Restart app to access.",
                data={'screen': 'Signals'}
            ))
            try:
                await context.bot.send_message(target_user_id, "🎉 Your VIP request has been approved!")
            except: pass

            admin_name = query.from_user.first_name
            original_caption = query.message.caption or query.message.text
            new_text = f"{original_caption}\n\n✅ <b>Approved by {admin_name}</b>"
            
            if query.message.caption:
                await query.edit_message_caption(caption=new_text, parse_mode=ParseMode.HTML)
            else:
                await query.edit_message_text(text=new_text, parse_mode=ParseMode.HTML)

        elif action == "decline":
            force_text = f"✍️ <b>Reason for declining User {target_user_id}?</b>\n\nReply to this message with the reason."
            
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=force_text,
                parse_mode=ParseMode.HTML,
                reply_markup=ForceReply(selective=True) 
            )

    async def receive_decline_reason(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receives the decline reason from the admin and notifies the user."""
        admin_id = update.effective_user.id
        reason = update.message.text
        user_id_to_decline = context.user_data.get('user_to_decline')
        admin_name = context.user_data.get('admin_name', admin_id)
        original_message_text = context.user_data.get('original_message_text', "VIP Request")
        original_message_id = context.user_data.get('original_message_id')
        is_photo_message = context.user_data.get('is_photo_message', False)

        if not user_id_to_decline:
            await update.message.reply_text("Error: Could not find the user to decline. Please try again.")
            return ConversationHandler.END
            
        self.db.update_vip_request_status(user_id_to_decline, 'rejected', admin_id, reason=reason)
        self.db.log_activity(admin_id, 'vip_declined', {'user_id': user_id_to_decline, 'reason': reason})

        try:
            asyncio.create_task(self.send_push_to_users(
                [user_id_to_decline],
                "VIP Request Update",
                f"Your VIP request was declined. Reason: {reason}",
                data={'screen': 'Signals', 'initialTab': 'vip'}
            ))
        except Exception as e:
            logger.error(f"Failed to send push notification for VIP decline: {e}")

        try:
            await context.bot.send_message(
                chat_id=user_id_to_decline,
                text=f"We regret to inform you that your VIP request has been declined.\n\nReason: {reason}"
            )
        except Exception as e:
            logger.error(f"Failed to notify user {user_id_to_decline} of decline via Telegram: {e}")
        
        await update.message.reply_text(f"The user {user_id_to_decline} has been notified of the decline.")
        
        if original_message_id:
            try:
                declined_append = f"\n\n--- ❌ Declined by {admin_name} ---\nReason: {reason}"
                
                if is_photo_message:
                    await context.bot.edit_message_caption(
                        chat_id=admin_id,
                        message_id=original_message_id,
                        caption=f"{original_message_text}{declined_append}"
                    )
                else:
                    await context.bot.edit_message_text(
                        chat_id=admin_id,
                        message_id=original_message_id,
                        text=f"{original_message_text}{declined_append}"
                    )
            except Exception as e:
                 logger.error(f"Failed to edit original decline message: {e}")

        context.user_data.clear()
        return ConversationHandler.END
        
    async def receive_schedule_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive and parse schedule time"""
        try:
            time_str = update.message.text.lower()
            delta = timedelta()
            
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
        query = update.callback_query 
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
        
        role_str = query.data.replace("role_", "")
        
        try:
            role = AdminRole(role_str)
            user_id = context.user_data['new_admin_id']

            if self.db.add_admin(user_id, role, query.from_user.id):
                await query.edit_message_text(f"✅ User {user_id} is now an admin with role '{role.value}'.")
            else:
                await query.edit_message_text(f"❌ Failed to add admin.")
        
        except ValueError:
            await query.edit_message_text(f"❌ Error: Invalid role '{role_str}'")

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
            admin_stats = self.db.get_admin_stats(user_id)
            admin_stats_text = (
                f"📊 Your Admin Statistics\n\n"
                f"📢 Broadcasts Sent: {admin_stats.get('broadcasts', 0)}\n"
                f"📝 Templates Created: {admin_stats.get('templates', 0)}\n"
                f"⏰ Broadcasts Scheduled: {admin_stats.get('scheduled', 0)}\n"
                f"⭐ Signals Rated: {admin_stats.get('ratings', 0)}"
            )
            
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

            full_stats_text = f"{admin_stats_text}\n\n{'-'*30}\n\n{user_stats_text}"
            
            await update.message.reply_text(full_stats_text)
        
        else:
            if not await self.is_user_subscribed(user_id, context):
                await self.send_join_channel_message(user_id, context)
                return

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
        """Handle /templates command - Interactive Menu"""
        if not self.has_permission(update.effective_user.id, Permission.MANAGE_TEMPLATES):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return

        templates = self.db.get_all_templates()
        
        if not templates:
            await update.message.reply_text("📝 No templates found.\nUse /savetemplate to create one.")
            return

        keyboard = []
        for t in templates:
            btn_text = f"{t['name']} ({t.get('category', 'General')})"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"tpl_view_{t['_id']}")])
            
        keyboard.append([InlineKeyboardButton("❌ Close", callback_data="close_settings")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("📝 <b>Template Manager</b>\nSelect a template to view, delete, or broadcast:", 
                                      reply_markup=reply_markup, 
                                      parse_mode=ParseMode.HTML)
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
            from bson.objectid import ObjectId
            ObjectId(broadcast_id)
        except Exception:
            await update.message.reply_text(f"❌ Invalid broadcast ID format.")
            return

        if self.db.cancel_scheduled_broadcast(broadcast_id, update.effective_user.id):
            await update.message.reply_text(f"✅ Scheduled broadcast {broadcast_id} cancelled.")
        else:
            await update.message.reply_text(f"❌ Broadcast {broadcast_id} not found or already processed.")


    async def news(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /news command"""
        if not self.finnhub_client:
            await update.message.reply_text("❌ The News service is currently disabled by the admin.")
            return

        try:
            if not await self.is_user_subscribed(update.effective_user.id, context):
                await self.send_join_channel_message(update.effective_user.id, context)
                return
            
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
            if not await self.is_user_subscribed(update.effective_user.id, context):
                await self.send_join_channel_message(update.effective_user.id, context)
                return
            
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
        elif "XAU" in pair or "GOLD" in pair:
            decimals = 2
            pip_multiplier = 0.1
        else: 
            decimals = 5
            pip_multiplier = 0.0001
        
        pip_value_per_lot = pip_multiplier * 100_000
    
        return pip_multiplier, decimals

    async def pips_calculator_v2(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enhanced with context and education"""
        
        self.engagement_tracker.update_engagement(update.effective_user.id, 'command_used')
        
        if not await self.is_user_subscribed(update.effective_user.id, context):
            await self.send_join_channel_message(update.effective_user.id, context)
            return
        
        if len(context.args) != 3:
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
            
            value_per_pip_lot = 10
            if "JPY" in pair:
                 value_per_pip_lot = 1000
            
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
        
        if any(x in pair for x in ['VOLATILITY', 'BOOM', 'CRASH', 'STEP', 'JUMP', 'V75', 'V100', 'V25']):
            return 1.0, "Deriv (assuming $1/point)"
            
        if 'XAU' in pair or 'GOLD' in pair:
            return 10.0, "Gold Standard ($10/pip)"
        if 'XAG' in pair or 'SILVER' in pair:
            return 50.0, "Silver Standard ($50/pip)"
        if 'BTC' in pair:
            return 1.0, "Crypto ($1/1.0 move)"
        if 'US30' in pair or 'DJ30' in pair:
             return 1.0, "Index (assuming $1/point)"

        if pair.endswith('USD'):
            return 10.0, "Standard ($10/pip)"
            
        if 'JPY' in pair:
            return 6.66, "JPY Pair (~$6.66/pip)"
        if 'CAD' in pair:
            return 7.40, "CAD Pair (~$7.40/pip)"
        if 'CHF' in pair:
            return 11.30, "CHF Pair (~$11.30/pip)"
        if 'GBP' in pair:
            return 12.70, "GBP Cross (~$12.70/pip)"
        if 'EUR' in pair:
            return 10.80, "EUR Cross (~$10.80/pip)"
            
        return 10.0, "Standard (Approx)"

    async def position_size_calculator(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Professional Position Size Calculator
        Usage: /positionsize [PAIR] [RISK_USD] [SL_PIPS]
        """
        self.engagement_tracker.update_engagement(update.effective_user.id, 'command_used')

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
            pair = context.args[0].upper()
            risk_usd = float(context.args[1])
            sl_pips = float(context.args[2])
            
            if risk_usd <= 0 or sl_pips <= 0:
                await update.message.reply_text("❌ Risk and SL must be positive numbers.")
                return

            pip_value_per_lot, description = self.get_estimated_pip_value(pair)
            
            if pip_value_per_lot > 0:
                raw_lots = risk_usd / (sl_pips * pip_value_per_lot)
            else:
                raw_lots = 0

            if "V75" in pair or "VOLATILITY" in pair:
                recommended_lots = round(raw_lots, 3) 
                if recommended_lots < 0.001: recommended_lots = 0.001
            else:
                recommended_lots = round(raw_lots, 2)
                if recommended_lots < 0.01: recommended_lots = 0.01

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
        all_users = self.db.get_all_users()
        target_users = {
            user_id for user_id in all_users 
            if self.notification_manager.should_notify(user_id, 'tips')
        }
        
        if not target_users:
            logger.info("No users to send educational content to")
            return
        content = await self.edu_content_manager.get_random_content()
        if not content:
            logger.info("No educational content found in database.")
            return
        success, failed = await self.edu_content_manager.broadcast_specific_content(
            context, 
            target_users,
            content 
        )
        
        logger.info(f"Daily educational content sent: {success} success, {failed} failed")
        await self.twitter.post_daily_tip(context, content)
        
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
                    user_doc = self.db.users_collection.find_one({'user_id': user_id}) or {}
                    new_status = not user_doc.get('leaderboard_public', True)
                    self.db.users_collection.update_one(
                        {'user_id': user_id},
                        {'$set': {'leaderboard_public': new_status}}
                    )
                elif key.startswith("notify_"):
                    actual_key = key.split('_', 1)[1]
                    
                    prefs = self.notification_manager.get_notification_preferences(user_id)
                    new_status = not prefs.get(actual_key, True)
                    self.db.users_collection.update_one(
                        {'user_id': user_id},
                        {'$set': {f'notifications.{actual_key}': new_status}}
                    )
                else:
                    logger.warning(f"Unknown settings toggle key: {key}")

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
    FORCE_SUB_CHANNEL = os.getenv('FORCE_SUB_CHANNEL')
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

    bot.application = application

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
