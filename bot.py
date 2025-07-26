import os
import json
import logging
from typing import Dict, List, Optional
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

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
WAITING_MESSAGE, WAITING_BUTTONS, WAITING_TARGET = range(3)

class BroadcastBot:
    def __init__(self, token: str, admin_ids: List[int]):
        self.token = token
        self.admin_ids = admin_ids
        self.subscribers = set()
        self.all_users = set()
        self.broadcast_data = {}
        
        # Load data from file if exists
        self.load_data()
    
    def save_data(self):
        """Save subscribers and users data to file"""
        data = {
            'subscribers': list(self.subscribers),
            'all_users': list(self.all_users)
        }
        with open('bot_data.json', 'w') as f:
            json.dump(data, f)
    
    def load_data(self):
        """Load subscribers and users data from file"""
        try:
            with open('bot_data.json', 'r') as f:
                data = json.load(f)
                self.subscribers = set(data.get('subscribers', []))
                self.all_users = set(data.get('all_users', []))
        except FileNotFoundError:
            logger.info("No existing data file found, starting fresh")
    
    def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        return user_id in self.admin_ids
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user_id = update.effective_user.id
        self.all_users.add(user_id)
        self.save_data()
        
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
                "Type /subscribe to get notified of broadcasts."
            )
            await update.message.reply_text(message)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        await self.start(update, context)
    
    async def subscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribe command"""
        user_id = update.effective_user.id
        self.all_users.add(user_id)
        
        if user_id in self.subscribers:
            await update.message.reply_text("✅ You're already subscribed to broadcasts!")
        else:
            self.subscribers.add(user_id)
            self.save_data()
            await update.message.reply_text("🔔 Successfully subscribed to broadcasts!")
    
    async def unsubscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /unsubscribe command"""
        user_id = update.effective_user.id
        
        if user_id in self.subscribers:
            self.subscribers.remove(user_id)
            self.save_data()
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
            self.subscribers.add(user_id)
            self.all_users.add(user_id)
            self.save_data()
            await update.message.reply_text(f"✅ User {user_id} added to subscribers list!")
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. Please provide a numeric ID.")
    
    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /stats command - Admin only"""
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return
        
        stats_text = (
            f"📊 Bot Statistics\n\n"
            f"👥 Total Users: {len(self.all_users)}\n"
            f"🔔 Subscribers: {len(self.subscribers)}\n"
            f"🔕 Non-subscribers: {len(self.all_users - self.subscribers)}"
        )
        await update.message.reply_text(stats_text)
    
    async def list_subscribers(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribers command - Admin only"""
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ You don't have permission to use this command.")
            return
        
        if not self.subscribers:
            await update.message.reply_text("📝 No subscribers yet.")
            return
        
        subscribers_list = "\n".join([f"• {sub_id}" for sub_id in sorted(self.subscribers)])
        message = f"📝 Subscribers List ({len(self.subscribers)} total):\n\n{subscribers_list}"
        
        # Telegram has a message length limit, so split if too long
        if len(message) > 4000:
            chunks = [subscribers_list[i:i+3500] for i in range(0, len(subscribers_list), 3500)]
            await update.message.reply_text(f"📝 Subscribers List ({len(self.subscribers)} total):")
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
        # Store the message
        context.user_data['broadcast_message'] = update.message
        
        # Ask about inline buttons
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
            return await self.ask_target_audience(update, context)
    
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
        
        return await self.ask_target_audience(update, context)
    
    async def ask_target_audience(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask who to send the broadcast to"""
        keyboard = [
            [InlineKeyboardButton("👥 All Users", callback_data="target_all")],
            [InlineKeyboardButton("🔔 Subscribers Only", callback_data="target_subscribers")],
            [InlineKeyboardButton("🔕 Non-subscribers Only", callback_data="target_nonsubscribers")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        stats_text = (
            f"👥 All Users: {len(self.all_users)}\n"
            f"🔔 Subscribers: {len(self.subscribers)}\n"
            f"🔕 Non-subscribers: {len(self.all_users - self.subscribers)}"
        )
        
        message = f"🎯 Choose Target Audience\n\n{stats_text}\n\nWho should receive this broadcast?"
        
        # Use appropriate method based on whether this is a callback or message
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
        if query.data == "target_all":
            target_users = self.all_users
            audience_name = "All Users"
        elif query.data == "target_subscribers":
            target_users = self.subscribers
            audience_name = "Subscribers"
        else:  # target_nonsubscribers
            target_users = self.all_users - self.subscribers
            audience_name = "Non-subscribers"
        
        if not target_users:
            await query.edit_message_text(f"❌ No {audience_name.lower()} found to send the broadcast to.")
            return ConversationHandler.END
        
        # Start broadcasting
        message = (
            f"📡 Broadcasting to {audience_name}\n\n"
            f"Sending to {len(target_users)} users...\n"
            f"This may take a few moments."
        )
        await query.edit_message_text(message)
        
        broadcast_message = context.user_data['broadcast_message']
        inline_buttons = context.user_data.get('inline_buttons')
        
        success_count = 0
        failed_count = 0
        
        for user_id in target_users:
            try:
                # Handle different message types
                if broadcast_message.text:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=broadcast_message.text,
                        reply_markup=inline_buttons
                    )
                elif broadcast_message.photo:
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=broadcast_message.photo[-1].file_id,
                        caption=broadcast_message.caption,
                        reply_markup=inline_buttons
                    )
                elif broadcast_message.video:
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=broadcast_message.video.file_id,
                        caption=broadcast_message.caption,
                        reply_markup=inline_buttons
                    )
                elif broadcast_message.document:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=broadcast_message.document.file_id,
                        caption=broadcast_message.caption,
                        reply_markup=inline_buttons
                    )
                elif broadcast_message.audio:
                    await context.bot.send_audio(
                        chat_id=user_id,
                        audio=broadcast_message.audio.file_id,
                        caption=broadcast_message.caption,
                        reply_markup=inline_buttons
                    )
                
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to send message to {user_id}: {e}")
                failed_count += 1
        
        # Send summary
        summary = (
            f"✅ Broadcast Complete!\n\n"
            f"📊 Results:\n"
            f"• Target: {audience_name}\n"
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
        
        # Try to notify the user about the error
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ Sorry, an error occurred while processing your request. Please try again."
                )
            except Exception:
                # If we can't send a message, just log it
                logger.error("Could not send error message to user")
    
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
    
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN environment variable is required")
    
    if not ADMIN_IDS or ADMIN_IDS == ['']:
        raise ValueError("ADMIN_IDS environment variable is required")
    
    try:
        admin_ids = [int(admin_id.strip()) for admin_id in ADMIN_IDS if admin_id.strip()]
    except ValueError:
        raise ValueError("ADMIN_IDS must be comma-separated list of integers")
    
    # Create and run bot
    bot = BroadcastBot(BOT_TOKEN, admin_ids)
    application = bot.create_application()
    
    # Get port from environment (Koyeb sets this)
    port = int(os.getenv('PORT', 8000))
    
    logger.info(f"Starting bot with {len(admin_ids)} admin(s)")
    logger.info(f"Bot will run on port {port}")
    
    # Run the bot
    application.run_polling()

if __name__ == '__main__':
    main()
