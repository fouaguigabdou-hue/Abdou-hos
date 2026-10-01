from flask import Flask, request, jsonify, render_template, make_response
import asyncio, json, os, threading, time
import auth
import main as bot
import packet_helpers as ph

app = Flask(__name__)

# ---------- asyncio loop ----------
loop = asyncio.new_event_loop()
def _loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()
threading.Thread(target=_loop, daemon=True).start()

def run_async(coro, timeout=30):
    return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=timeout)

def spawn(coro):
    """يشغّل كوروتين في الخلفية بدون انتظار"""
    return asyncio.run_coroutine_threadsafe(coro, loop)


# ---------- Auth ----------
def current_user():
    tok = request.cookies.get("session_token") or request.headers.get("X-Session-Token")
    return auth.get_user(tok) if tok else None

def need_auth():
    u = current_user()
    if not u: return None, jsonify({'success': False, 'message': 'غير مسموح'}), 401
    return u, None, None

def need_admin():
    u = current_user()
    if not u or u.get("role") != "admin":
        return None, jsonify({'success': False, 'message': 'للمدير فقط'}), 403
    return u, None, None


# ================== صفحات ==================
@app.route('/')
def home():
    u = current_user()
    if not u: return render_template('login.html')
    if u["role"] == "admin": return render_template('admin.html')
    return render_template('dashboard.html')


@app.route('/login', methods=['POST'])
def login():
    d = request.json
    mode = d.get('mode')

    if mode == 'admin':
        if auth.check_admin_password(d.get('password', '')):
            r = make_response(jsonify({'success': True, 'role': 'admin'}))
            r.set_cookie('session_token', 'ADMIN', max_age=86400*7, httponly=True)
            return r
        return jsonify({'success': False, 'message': '❌ كلمة سر خاطئة'})

    if mode == 'code':
        code = d.get('code', '').strip().upper()
        if not code.startswith(auth.CODE_PREFIX):
            return jsonify({'success': False, 'message': f'❌ يجب أن يبدأ بـ {auth.CODE_PREFIX}'})
        ok, res = auth.validate_code(code)
        if not ok:
            return jsonify({'success': False, 'message': f'❌ {res}'})
        r = make_response(jsonify({'success': True, 'role': 'user'}))
        r.set_cookie('session_token', res, max_age=86400*30, httponly=True)
        return r

    return jsonify({'success': False, 'message': 'طلب خاطئ'})


@app.route('/logout', methods=['POST'])
def logout():
    r = make_response(jsonify({'success': True}))
    r.delete_cookie('session_token')
    return r


# ================== مدير: أكواد ==================
@app.route('/api/admin/generate_code', methods=['POST'])
def gen_code():
    _, err, code = need_admin()
    if err: return err, code
    d = request.json
    c = auth.generate_code(int(d.get('days', 30)), int(d.get('uses', 1)), d.get('note', ''))
    return jsonify({'success': True, 'code': c})


@app.route('/api/admin/codes')
def all_codes():
    _, err, code = need_admin()
    if err: return err, code
    return jsonify({'codes': auth.list_codes()})


@app.route('/api/admin/delete_code', methods=['POST'])
def del_code():
    _, err, code = need_admin()
    if err: return err, code
    return jsonify({'success': auth.delete_code(request.json['code'])})


@app.route('/api/admin/users')
def all_users():
    _, err, code = need_admin()
    if err: return err, code
    return jsonify({'users': auth.list_users()})


# ================== الحسابات الفرعية ==================
@app.route('/api/sub/add', methods=['POST'])
def sub_add():
    u, err, code = need_auth()
    if err: return err, code
    d = request.json
    ok, msg = bot.add_sub_account(d['uid'], d['password'], d.get('name', ''))
    return jsonify({'success': ok, 'message': msg})


@app.route('/api/sub/remove', methods=['POST'])
def sub_remove():
    u, err, code = need_auth()
    if err: return err, code
    ok, msg = bot.remove_sub_account(request.json['uid'])
    return jsonify({'success': ok, 'message': msg})


@app.route('/api/sub/list')
def sub_list():
    u, err, code = need_auth()
    if err: return err, code
    accounts = [{
        'uid': s.uid, 'name': s.name, 'connected': s.connected
    } for s in bot.sub_accounts.values()]
    return jsonify({'accounts': accounts})


@app.route('/api/sub/startall', methods=['POST'])
def sub_start_all():
    u, err, code = need_auth()
    if err: return err, code
    async def _r():
        for uid in list(bot.sub_accounts.keys()):
            await bot.start_sub_account(uid)
            await asyncio.sleep(2)
    spawn(_r())
    return jsonify({'message': '✓ جارٍ تشغيل كل الحسابات...'})


@app.route('/api/sub/stopall', methods=['POST'])
def sub_stop_all():
    u, err, code = need_auth()
    if err: return err, code
    async def _r():
        for uid in list(bot.sub_accounts.keys()):
            await bot.stop_sub_account(uid)
        await bot.stop_multi_squad_spam()
    spawn(_r())
    return jsonify({'message': '✓ جارٍ إيقاف الكل...'})


# ================== 📌 زر: جلب معلومات لاعب ==================
@app.route('/api/player/info', methods=['POST'])
def player_info():
    u, err, code = need_auth()
    if err: return err, code
    uid = str(request.json.get('uid', '')).strip()
    if not uid.isdigit():
        return jsonify({'success': False, 'message': '❌ UID غير صحيح'})

    ok, data = run_async(ph.fetch_player_info(uid), timeout=20)
    if not ok:
        return jsonify({'success': False, 'message': f'❌ {data}'})
    return jsonify({'success': True, 'data': data})


# ================== 📌 زر: فحص الحظر ==================
@app.route('/api/player/check', methods=['POST'])
def player_check():
    u, err, code = need_auth()
    if err: return err, code
    uid = str(request.json.get('uid', '')).strip()
    if not uid.isdigit():
        return jsonify({'success': False, 'message': '❌ UID غير صحيح'})
    ok, data = run_async(ph.fetch_check_ban(uid), timeout=20)
    if not ok:
        return jsonify({'success': False, 'message': f'❌ {data}'})
    return jsonify({'success': True, 'data': data})


# ================== 📌 زر: سبام انضمام للفريق (SPM) ==================
@app.route('/api/spam/join', methods=['POST'])
def spam_join():
    u, err, code = need_auth()
    if err: return err, code
    uid = str(request.json.get('uid', '')).strip()
    if not uid.isdigit():
        return jsonify({'success': False, 'message': '❌ UID غير صحيح'})

    ph.register_task(
        f"spm_{uid}",
        ph.squad_join_spam_loop(uid, bot.MajoRLoGinauThKey, bot.MajoRLoGinauThIv)
    )
    return jsonify({'message': f'🚀 بدأ سبام الانضمام على {uid}'})


@app.route('/api/spam/join/stop', methods=['POST'])
def spam_join_stop():
    u, err, code = need_auth()
    if err: return err, code
    uid = str(request.json.get('uid', '')).strip()
    ok = ph.stop_task(f"spm_{uid}")
    return jsonify({'message': '✓ تم الإيقاف' if ok else 'لا يوجد سبام نشط'})


# ================== 📌 زر: سبام Lag للفريق ==================
@app.route('/api/spam/lag', methods=['POST'])
def spam_lag():
    u, err, code = need_auth()
    if err: return err, code
    d = request.json
    teamcode = str(d.get('teamcode', '')).strip()
    count = int(d.get('count', 30))
    delay = float(d.get('delay', 0.25))
    if not teamcode:
        return jsonify({'success': False, 'message': '❌ أدخل teamcode'})

    ph.register_task(
        f"lag_{teamcode}",
        ph.lag_squad_loop(teamcode, bot.MajoRLoGinauThKey, bot.MajoRLoGinauThIv,
                          count=count, delay=delay)
    )
    return jsonify({'message': f'💥 بدأ lag على {teamcode} ({count} حزمة)'})


@app.route('/api/spam/lag/stop', methods=['POST'])
def spam_lag_stop():
    u, err, code = need_auth()
    if err: return err, code
    teamcode = str(request.json.get('teamcode', '')).strip()
    ok = ph.stop_task(f"lag_{teamcode}")
    return jsonify({'message': '✓ تم الإيقاف' if ok else 'لا يوجد lag نشط'})


# ================== 📌 زر: سبام رسائل للفريق ==================
@app.route('/api/spam/msg', methods=['POST'])
def spam_msg():
    u, err, code = need_auth()
    if err: return err, code
    d = request.json
    teamcode = str(d.get('teamcode', '')).strip()
    message = str(d.get('message', '')).strip()
    count = int(d.get('count', 30))
    delay = float(d.get('delay', 0.3))
    if not teamcode or not message:
        return jsonify({'success': False, 'message': '❌ أدخل teamcode والرسالة'})

    ph.register_task(
        f"msg_{teamcode}",
        ph.message_squad_loop(teamcode, message,
                              bot.MajoRLoGinauThKey, bot.MajoRLoGinauThIv,
                              count=count, delay=delay)
    )
    return jsonify({'message': f'💬 بدأ سبام الرسائل على {teamcode}'})


@app.route('/api/spam/msg/stop', methods=['POST'])
def spam_msg_stop():
    u, err, code = need_auth()
    if err: return err, code
    teamcode = str(request.json.get('teamcode', '')).strip()
    ok = ph.stop_task(f"msg_{teamcode}")
    return jsonify({'message': '✓ تم الإيقاف' if ok else 'لا يوجد سبام رسائل'})


# ================== 📌 زر: سبام الفريق المتعدد (كل الحسابات) ==================
@app.route('/api/spam/multi', methods=['POST'])
def spam_multi():
    u, err, code = need_auth()
    if err: return err, code
    d = request.json
    teamcode = str(d.get('teamcode', '')).strip()
    count = int(d.get('count', 30))
    delay = float(d.get('delay', 0.25))
    if not teamcode:
        return jsonify({'success': False, 'message': '❌ أدخل teamcode'})

    async def _r():
        # تأكد الحسابات متصلة
        for uid, s in bot.sub_accounts.items():
            if not s.connected:
                await bot.start_sub_account(uid)
                await asyncio.sleep(2)
        await bot.start_multi_squad_spam(teamcode, count, delay)

    spawn(_r())
    return jsonify({'message': f'🚀 كل الحسابات بدأت السبام على {teamcode}'})


@app.route('/api/spam/multi/stop', methods=['POST'])
def spam_multi_stop():
    u, err, code = need_auth()
    if err: return err, code
    spawn(bot.stop_multi_squad_spam())
    return jsonify({'message': '✓ تم إيقاف سبام الفريق المتعدد'})


# ================== 📌 زر: أشباح ==================
@app.route('/api/spam/ghost', methods=['POST'])
def spam_ghost():
    u, err, code = need_auth()
    if err: return err, code
    d = request.json
    teamcode = str(d.get('teamcode', '')).strip()
    name = str(d.get('name', 'Ghost')).strip()
    if not teamcode:
        return jsonify({'success': False, 'message': '❌ أدخل teamcode'})

    ok, msg = run_async(
        ph.ghost_squad_once(teamcode, name,
                            bot.MajoRLoGinauThKey, bot.MajoRLoGinauThIv),
        timeout=30
    )
    return jsonify({'success': ok, 'message': msg})


# ================== 📌 زر: حظر لاعب ==================
@app.route('/api/spam/ban', methods=['POST'])
def spam_ban():
    u, err, code = need_auth()
    if err: return err, code
    uid = str(request.json.get('uid', '')).strip()
    if not uid.isdigit():
        return jsonify({'success': False, 'message': '❌ UID غير صحيح'})

    ph.register_task(
        f"ban_{uid}",
        ph.ban_player_loop(uid, bot.MajoRLoGinauThKey, bot.MajoRLoGinauThIv)
    )
    return jsonify({'message': f'🔨 بدأ حظر {uid}'})


@app.route('/api/spam/ban/stop', methods=['POST'])
def spam_ban_stop():
    u, err, code = need_auth()
    if err: return err, code
    uid = str(request.json.get('uid', '')).strip()
    ok = ph.stop_task(f"ban_{uid}")
    return jsonify({'message': '✓ تم الإيقاف' if ok else 'لا يوجد حظر نشط'})


# ================== 📌 زر: إنشاء فريق 5 أو 6 ==================
@app.route('/api/squad/create', methods=['POST'])
def squad_create():
    u, err, code = need_auth()
    if err: return err, code
    d = request.json
    size = int(d.get('size', 5))
    uid = str(d.get('uid', '')).strip()
    if size not in (5, 6):
        return jsonify({'success': False, 'message': '❌ الحجم يجب 5 أو 6'})
    if not uid.isdigit():
        return jsonify({'success': False, 'message': '❌ UID غير صحيح'})

    ok, msg = run_async(
        ph.create_squad(size, uid,
                        bot.MajoRLoGinauThKey, bot.MajoRLoGinauThIv,
                        bot.region),
        timeout=30
    )
    return jsonify({'success': ok, 'message': msg})


# ================== 📌 زر: إيموتات Evo ==================
@app.route('/api/evo/start', methods=['POST'])
def evo_start():
    u, err, code = need_auth()
    if err: return err, code
    d = request.json
    uids = [str(x) for x in d.get('uids', []) if str(x).isdigit()]
    if not uids:
        return jsonify({'success': False, 'message': '❌ أدخل UID واحد على الأقل'})

    bot.evo_cycle_running = True
    bot.evo_cycle_task = loop.create_task(
        bot.evo_gun_cycle(uids, bot.EVO_IDS,
                          bot.MajoRLoGinauThKey, bot.MajoRLoGinauThIv,
                          bot.region)
    )
    return jsonify({'message': f'🎭 بدأ Evo Cycle على {len(uids)} لاعب'})


@app.route('/api/evo/stop', methods=['POST'])
def evo_stop():
    u, err, code = need_auth()
    if err: return err, code
    bot.evo_cycle_running = False
    if bot.evo_cycle_task and not bot.evo_cycle_task.done():
        loop.call_soon_threadsafe(bot.evo_cycle_task.cancel)
    return jsonify({'message': '✓ تم إيقاف Evo Cycle'})


# ================== 📌 إيقاف كل السبامات ==================
@app.route('/api/spam/stopall', methods=['POST'])
def spam_stop_all():
    u, err, code = need_auth()
    if err: return err, code
    ph.stop_all_tasks()
    spawn(bot.stop_multi_squad_spam())
    bot.evo_cycle_running = False
    return jsonify({'message': '✓ تم إيقاف كل السبامات'})


# ================== تشغيل البوت ==================
def start_bot():
    spawn(bot.MaiiiinE())
    spawn(bot.auto_start_all_subs())

start_bot()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)