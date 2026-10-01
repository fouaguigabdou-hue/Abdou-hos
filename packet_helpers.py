"""
كل دوال السبام والـ lag والمعلومات — جاهزة للاستخدام من API.
"""
import asyncio
import aiohttp
import main as bot  # ملف main.py تاعك


# ============================================================
#                   جلب معلومات اللاعب
# ============================================================
async def fetch_player_info(player_id):
    """يرجع معلومات اللاعب الكاملة"""
    try:
        url = f"https://nirob-x-info.vercel.app/info?uid={player_id}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, ssl=False, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    return True, await r.json()
                return False, f"HTTP {r.status}"
    except Exception as e:
        return False, str(e)


async def fetch_check_ban(player_id):
    """فحص الحظر"""
    try:
        url = f"https://ff.garena.com/api/antihack/check_banned?lang=en&uid={player_id}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 10)",
            "Accept": "application/json",
            "referer": "https://ff.garena.com/en/support/",
            "x-requested-with": "B6FksShzIgjfrYImLpTsadjS86sddhFH"
        }
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=headers, ssl=False,
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    return True, await r.json()
                return False, f"HTTP {r.status}"
    except Exception as e:
        return False, str(e)


# ============================================================
#                   سبام انضمام للفريق (Squad Join Spam)
# ============================================================
async def squad_join_spam_loop(target_uid, key, iv, interval=0.5):
    """حلقة ترسل دعوات انضمام لا نهائية للاعب — يعمل عبر قناة Online"""
    while True:
        try:
            if bot.online_writer is None:
                await asyncio.sleep(1)
                continue
            pkt = await bot.SPamSq(target_uid, key, iv)
            await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', pkt)
        except asyncio.CancelledError:
            print(f"[SPM] cancelled for {target_uid}")
            break
        except Exception as e:
            print(f"[SPM] error {target_uid}: {e}")
        await asyncio.sleep(interval)


# ============================================================
#                   سبام Lag (يدخل ويخرج بسرعة)
# ============================================================
async def lag_squad_loop(team_code, key, iv, count=30, delay=0.25):
    """ينضم للفريق ويرسل حزم lag بشكل متكرر"""
    while True:
        try:
            if bot.online_writer is None:
                await asyncio.sleep(1)
                continue

            # 1) انضم
            join = await bot.JoinSq_original(team_code, key, iv)
            await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', join)
            await asyncio.sleep(0.35)

            # 2) أرسل count حزمة lag
            for _ in range(count):
                try:
                    lag = await bot.xSKINZxLag(key, iv)
                    await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', lag)
                    await asyncio.sleep(delay)
                except Exception:
                    break

            # 3) اخرج
            try:
                leave = await bot.ExiT(None, key, iv)
                await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', leave)
            except Exception:
                pass

            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[LAG] {e}")
            await asyncio.sleep(1)


# ============================================================
#                   سبام رسائل داخل الفريق
# ============================================================
async def message_squad_loop(team_code, message, key, iv,
                              chat_type=1, uid=None, chat_id=None,
                              count=30, delay=0.3):
    """ينضم للفريق ويرسل count رسالة سبام"""
    while True:
        try:
            if bot.online_writer is None:
                await asyncio.sleep(1)
                continue

            join = await bot.JoinSq_original(team_code, key, iv)
            await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', join)
            await asyncio.sleep(1.2)

            for _ in range(count):
                try:
                    colored = f"[B][C]{bot.get_random_color()}{message}"
                    pkt = await bot.xSEndMsgsQ(colored, chat_id or 0, key, iv)
                    await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'ChaT', pkt)
                    await asyncio.sleep(delay)
                except Exception:
                    break

            try:
                leave = await bot.ExiT(None, key, iv)
                await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', leave)
            except Exception:
                pass

            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[MSG] {e}")
            await asyncio.sleep(1)


# ============================================================
#                   أشباح (Ghosts)
# ============================================================
async def ghost_squad_once(team_code, name, key, iv):
    """يرسل 4 أشباح للفريق (مرة واحدة)"""
    try:
        join = await bot.JoinSq_original(team_code, key, iv)
        await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', join)
        await asyncio.sleep(2)

        try:
            leave = await bot.ExiT(None, key, iv)
            await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', leave)
        except Exception:
            pass

        await asyncio.sleep(0.2)
        for _ in range(4):
            g = await bot.GhostPakcet(bot.bot_uid, name,
                                      bot.ghost_squad_code or "000000",
                                      key, iv)
            await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', g)
            await asyncio.sleep(0.1)
        return True, "✓ تم إرسال الأشباح"
    except Exception as e:
        return False, str(e)


# ============================================================
#                   حظر لاعب (Ban Loop)
# ============================================================
async def ban_player_loop(target_uid, key, iv, interval=0.5):
    """حلقة إرسال حزم الحظر"""
    while True:
        try:
            if bot.online_writer is None:
                await asyncio.sleep(1)
                continue
            pkt = await bot.xBaNchaTxSkInZ(target_uid, key, iv)
            await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', pkt)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[BAN] {e}")
        await asyncio.sleep(interval)


# ============================================================
#                   إنشاء فريق 5 أو 6
# ============================================================
async def create_squad(size, target_uid, key, iv, region):
    """ينشئ فريق من 5 أو 6 لاعبين ويدعو الهدف"""
    try:
        # رسالة أولى
        chat_type, chat_id = 3, 0
        msg = f"[B][C]{bot.get_random_color()}\n\nPleaSe AccepT My InViTe In 3 SeConDs!!\n\n"
        P = await bot.xSEndMsgsQ(msg, 0, key, iv)
        await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'ChaT', P)

        # اخرج من أي فريق حالي
        leave = await bot.ExiT(target_uid, key, iv)
        await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', leave)
        await asyncio.sleep(0.5)

        # افتح فريق
        open_pkt = await bot.OpEnSq(key, iv, region)
        await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', open_pkt)

        # غيّر الحجم
        c = await bot.cHSq(size, target_uid, key, iv, region)
        await asyncio.sleep(0.5)
        await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', c)

        # أرسل الدعوة
        v = await bot.SEnd_InV(size, target_uid, key, iv, region)
        await asyncio.sleep(0.5)
        await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', v)

        # انتظر ثم اخرج
        await asyncio.sleep(3.5)
        e = await bot.ExiT(None, key, iv)
        await bot.SEndPacKeT(bot.whisper_writer, bot.online_writer, 'OnLine', e)

        return True, f"✓ تم إنشاء فريق {size}"
    except Exception as e:
        return False, str(e)


# ============================================================
#                   سجل المهام النشطة
# ============================================================
active_tasks = {}   # {task_id: asyncio.Task}


def register_task(task_id, coro):
    """يسجّل مهمة جديدة وينظف القديمة إن وجدت"""
    old = active_tasks.get(task_id)
    if old and not old.done():
        old.cancel()
    active_tasks[task_id] = asyncio.create_task(coro)
    return active_tasks[task_id]


def stop_task(task_id):
    t = active_tasks.get(task_id)
    if t and not t.done():
        t.cancel()
        del active_tasks[task_id]
        return True
    return False


def stop_all_tasks():
    for tid, t in list(active_tasks.items()):
        t.cancel()
        del active_tasks[tid]