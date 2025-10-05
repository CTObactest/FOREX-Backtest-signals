import os
import logging
import asyncio
from typing import Dict, List, Optional
from aiohttp import web
import threading
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

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
WAITING_MESSAGE, WAITING_BUTTONS, WAITING_PROTECTION, WAITING_TARGET = range(4)

class MongoDBHandler:
    """Handle all MongoDB operations"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.client = None
        self.db = None
        self.users_collection = None
        self.subscribers_collection = None
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
            
            # Create indexes for better performance
            self.users_collection.create_index('user_id', unique=True)
            self.subscribers_collection.create_index('user_id', unique=True)
            
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
                        'last_interaction': asyncio.get_event_loop().time()
                    },
                    '$setOnInsert': {'created_at': asyncio.get_event_loop().time()}
                },
                upsert=True
            )
            logger.info(f"User {user_id} added/updated in database")
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
                        'subscribed_at': asyncio.get_event_loop().time()
                    }
                },
                upsert=True
            )
            logger.info(f"User {user_id} subscribed")
            return True
        except Exception as e:
            logger.error(f"Error subscribing user {user_id}: {e}")
            return False
    
    def remove_subscriber(self, user_id: int):
        """Remove a subscriber"""
        try:
            result = self.subscribers_collection.delete_one({'user_id': user_id})
            if result.deleted_count > 0:
                logger.info(f"User {user_id} unsubscribed")
                return True
            return False
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
            return {
                'total_users': total_users,
                'subscribers': total_subscribers,
                'non_subscribers': total_users - total_subscribers
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {'total_users': 0, 'subscribers': 0, 'non_subscribers': 0}
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


class BroadcastBot:
    def __init__(self, token: str, admin_ids: List[int], mongo_handler: MongoDBHandler):
        self.token = token
        self.admin_ids = admin_ids
        self.db = mongo_handler
    
    def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        return user_id in self.admin_ids
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user = update.effective_user
        user_id = user.id
        
        # Save user to database
        self.db.add_user(user_id, user.username, user.first_name)
        
        if self.is_admin(user_id):
            message = (
                "🔧 Admin Panel\n\n"
                "Available commands:\n"
                "/broadcast - Start broadcasting a message\n"
                "/add <user_id> - Add user to subscribers\n"
                "/stats - View bot statistics\n"
                "/subscribers - List all subscribers\n"
                "/help - Show this help message"
            )
            await update.message.reply_text(message)
        else:
            message = (
                "👋 Welcome to PipSage — wise signals, steady gains!\n\n"
                "You'll get curated trade signals and VIP updates here.\n"
                "Enable notifications to get notified of broadcasts."
            )
            await update.message.reply_text(message)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        await self.start(update, context)
    
    async def subscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribe command"""
        user = update.effective_user
        user_id = user.id
        
        # Ensure user exists in database
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
    
    async def add_subscriber(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /add command - Admin only"""
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return
        
        if not context.args:
            await update.message.reply_text("❌ Please provide a user ID: /add <user_id>")
            return
        
        try:
            user_id = int(context.args[0])
            self.db.add_user(user_id)
            self.db.add_subscriber(user_id)
            await update.message.reply_text(f"✅ User {user_id} added to subscribers list!")
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. Please provide a numeric ID.")
    
    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command - Admin only"""
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return
        
        stats = self.db.get_stats()
        stats_text = (
            f"📊 Bot Statistics\n\n"
            f"👥 Total Users: {stats['total_users']}\n"
            f"🔔 Subscribers: {stats['subscribers']}\n"
            f"🔕 Non-subscribers: {stats['non_subscribers']}"
        )
        await update.message.reply_text(stats_text)
    
    async def list_subscribers(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribers command - Admin only"""
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return
        
        subscribers = self.db.get_all_subscribers()
        
        if not subscribers:
            await update.message.reply_text("📝 No subscribers yet.")
            return
        
        subscribers_list = "\n".join([f"• {sub_id}" for sub_id in sorted(subscribers)])
        message = f"📝 Subscribers List ({len(subscribers)} total):\n\n{subscribers_list}"
        
        # Telegram has a message length limit, so split if too long
        if len(message) > 4000:
            chunks = [subscribers_list[i:i+3500] for i in range(0, len(subscribers_list), 3500)]
            await update.message.reply_text(f"📝 Subscribers List ({len(subscribers)} total):")
            for chunk in chunks:
                await update.message.reply_text(chunk)
        else:
            await update.message.reply_text(message)
    
    async def start_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start broadcast conversation - Admin only"""
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return ConversationHandler.END
        
        message = (
            "📝 Start Broadcasting\n\n"
            "Please send me the message you want to broadcast.\n"
            "You can send text, photos, videos, or documents.\n\n"
            "Send /cancel to cancel this operation."
        )
        await update.message.reply_text(message)
        return WAITING_MESSAGE
    
    async def receive_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive the broadcast message"""
        context.user_data['broadcast_message'] = update.message
        
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
            [InlineKeyboardButton("🔒 Protect Message (No forwarding/sharing)", callback_data="protect_yes")],
            [InlineKeyboardButton("🔓 Allow Forwarding/Sharing", callback_data="protect_no")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = (
            "🛡️ Message Protection Settings\n\n"
            "Choose protection level for your broadcast:\n\n"
            "🔒 **Protect Message**: Prevents forwarding, copying, and sharing\n"
            "• Recipients cannot forward the message\n"
            "• Text cannot be selected/copied\n"
            "• Screenshots are restricted (in some clients)\n\n"
            "🔓 **Allow Forwarding**: Normal message behavior\n"
            "• Recipients can forward and share\n"
            "• Text can be copied\n"
            "• No restrictions"
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
        
        if query.data == "protect_yes":
            context.user_data['protect_content'] = True
            protection_status = "🔒 Message will be protected (no forwarding/sharing)"
        else:
            context.user_data['protect_content'] = False
            protection_status = "🔓 Message will allow forwarding/sharing"
        
        await query.edit_message_text(f"✅ {protection_status}")
        
        return await self.ask_target_audience(update, context)
    
    async def ask_target_audience(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        else:
            await update.message.reply_text(message, reply_markup=reply_markup)
        
        return WAITING_TARGET
    
    async def handle_target_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle target audience choice and send broadcast"""
        query = update.callback_query
        await query.answer()
        
        # Determine target users
        all_users = self.db.get_all_users()
        subscribers = self.db.get_all_subscribers()
        
        if query.data == "target_all":
            target_users = all_users
            audience_name = "All Users"
        elif query.data == "target_subscribers":
            target_users = subscribers
            audience_name = "Subscribers"
        else:  # target_nonsubscribers
            target_users = all_users - subscribers
            audience_name = "Non-subscribers"
        
        if not target_users:
            await query.edit_message_text(f"❌ No {audience_name.lower()} found to send the broadcast to.")
            return ConversationHandler.END
        
        # Get broadcast settings
        broadcast_message = context.user_data['broadcast_message']
        inline_buttons = context.user_data.get('inline_buttons')
        protect_content = context.user_data.get('protect_content', False)
        
        protection_status = "🔒 Protected" if protect_content else "🔓 Unprotected"
        
        # Start broadcasting
        message = (
            f"📡 Broadcasting to {audience_name}\n\n"
            f"Settings:\n"
            f"• Target: {audience_name} ({len(target_users)} users)\n"
            f"• Protection: {protection_status}\n"
            f"• Buttons: {'Yes' if inline_buttons else 'No'}\n\n"
            f"Sending messages...\n"
            f"This may take a few moments."
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
                elif broadcast_message.audio:
                    await context.bot.send_audio(
                        chat_id=user_id,
                        audio=broadcast_message.audio.file_id,
                        caption=broadcast_message.caption,
                        reply_markup=inline_buttons,
                        protect_content=protect_content
                    )
                
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to send message to {user_id}: {e}")
                failed_count += 1
        
        # Send summary
        protection_summary = "🔒 Protected (No forwarding/sharing)" if protect_content else "🔓 Unprotected (Forwarding allowed)"
        summary = (
            f"✅ Broadcast Complete!\n\n"
            f"📊 Results:\n"
            f"• Target: {audience_name}\n"
            f"• Protection: {protection_summary}\n"
            f"• Successfully sent: {success_count}\n"
            f"• Failed: {failed_count}\n"
            f"• Total attempted: {len(target_users)}"
        )
        
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=summary
        )
        
        return ConversationHandler.END
    
    async def cancel_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel broadcast operation"""
        await update.message.reply_text("❌ Broadcast operation cancelled.")
        return ConversationHandler.END
    
    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle errors"""
        logger.error(f"Update {update} caused error {context.error}")
        
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ Sorry, an error occurred while processing your request. Please try again."
                )
            except Exception:
                logger.error("Could not send error message to user")
    
    def create_health_server(self, port: int):
        """Create a simple health check server"""
        async def health_check(request):
            return web.Response(text="OK", status=200)
        
        async def root_handler(request):
            return web.Response(text="Telegram Bot is running", status=200)
        
        app = web.Application()
        app.router.add_get('/health', health_check)
        app.router.add_get('/', root_handler)
        
        return app
    
    def run_health_server(self, port: int):
        """Run the health check server in a separate thread"""
        async def start_server():
            app = self.create_health_server(port)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', port)
            await site.start()
            logger.info(f"Health check server started on port {port}")
            
            while True:
                await asyncio.sleep(1)
        
        def run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(start_server())
        
        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()
    
    def create_application(self):
        """Create and configure the application"""
        application = Application.builder().token(self.token).build()
        
        # Broadcast conversation handler
        broadcast_handler = ConversationHandler(
            entry_points=[CommandHandler("broadcast", self.start_broadcast)],
            states={
                WAITING_MESSAGE: [
                    MessageHandler(filters.ALL & ~filters.COMMAND, self.receive_broadcast_message)
                ],
                WAITING_BUTTONS: [
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
        
        # Command handlers
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help_command))
        application.add_handler(CommandHandler("subscribe", self.subscribe))
        application.add_handler(CommandHandler("unsubscribe", self.unsubscribe))
        application.add_handler(CommandHandler("add", self.add_subscriber))
        application.add_handler(CommandHandler("stats", self.stats))
        application.add_handler(CommandHandler("subscribers", self.list_subscribers))
        application.add_handler(broadcast_handler)
        
        # Add error handler
        application.add_error_handler(self.error_handler)
        
        return application


def main():
    """Main function to run the bot"""
    # Get configuration from environment variables
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    ADMIN_IDS = os.getenv('ADMIN_IDS', '').split(',')
    MONGODB_URI = os.getenv('MONGODB_URI')
    
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN environment variable is required")
    
    if not ADMIN_IDS or ADMIN_IDS == ['']:
        raise ValueError("ADMIN_IDS environment variable is required")
    
    if not MONGODB_URI:
        raise ValueError("MONGODB_URI environment variable is required")
    
    try:
        admin_ids = [int(admin_id.strip()) for admin_id in ADMIN_IDS if admin_id.strip()]
    except ValueError:
        raise ValueError("ADMIN_IDS must be comma-separated list of integers")
    
    # Initialize MongoDB connection
    logger.info("Connecting to MongoDB...")
    mongo_handler = MongoDBHandler(MONGODB_URI)
    
    # Create bot
    bot = BroadcastBot(BOT_TOKEN, admin_ids, mongo_handler)
    application = bot.create_application()
    
    # Get port from environment
    port = int(os.getenv('PORT', 8000))
    
    logger.info(f"Starting bot with {len(admin_ids)} admin(s)")
    logger.info(f"Health server will run on port {port}")
    
    # Start health check server
    bot.run_health_server(port)
    
    # Small delay to ensure health server starts
    import time
    time.sleep(2)
    
    # Run the bot
    logger.info("Starting Telegram bot polling...")
    try:
        application.run_polling()
    finally:
        # Clean up MongoDB connection
        mongo_handler.close()


if __name__ == '__main__':
    main()
