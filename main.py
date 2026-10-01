# اجعل المفاتيح عامة يستطيع app.py الوصول لها
MajoRLoGinauThKey = None
MajoRLoGinauThIv = None
import requests, os, psutil, sys, jwt, pickle, json, binascii, time, urllib3, base64, datetime, re, socket, threading, ssl, pytz, aiohttp, random, asyncio
from protobuf_decoder.protobuf_decoder import Parser
from xC4 import *; from xHeaders import *
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from Pb2 import DEcwHisPErMsG_pb2, MajoRLoGinrEs_pb2, PorTs_pb2, MajoRLoGinrEq_pb2, sQ_pb2, Team_msg_pb2
from cfonts import render, say
from xP import JoinSq, OpenCh, MsqSq, ExitSq
import RemoveFriend_Req_pb2
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from byte import Encrypt_ID, encrypt_api

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SPAM_TARGETS_FILE = "spam_targets.json"
SUB_ACCOUNTS_FILE = "sub_accounts.json"

# ==================================================================
#   SUB ACCOUNTS (Multi-Account Squad Spam System)
# ==================================================================
class SubAccount:
    def __init__(self, uid, password, name=""):
        self.uid = uid
        self.password = password
        self.name = name or f"Sub_{str(uid)[-4:]}"
        self.token = None
        self.key = None
        self.iv = None
        self.region = None
        self.bot_uid = None
        self.timestamp = None
        self.url = None
        self.chat_auth = None
        self.online_auth = None
        self.online_writer = None
        self.whisper_writer = None
        self.online_ip = None
        self.online_port = None
        self.chat_ip = None
        self.chat_port = None
        self.connected = False
        self.ready_event = asyncio.Event()
        self.tasks = []


sub_accounts = {}
sub_spam_tasks = {}
active_sub_spam = False


def load_sub_accounts():
    global sub_accounts
    if not os.path.exists(SUB_ACCOUNTS_FILE):
        return
    try:
        with open(SUB_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for entry in data:
            sub = SubAccount(entry["uid"], entry["password"], entry.get("name", ""))
            sub_accounts[entry["uid"]] = sub
        print(f"[SUB] Loaded {len(sub_accounts)} sub account(s)")
    except Exception as e:
        print(f"[SUB] Load error: {e}")


def save_sub_accounts():
    data = [
        {"uid": s.uid, "password": s.password, "name": s.name}
        for s in sub_accounts.values()
    ]
    with open(SUB_ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[SUB] Saved {len(data)} sub account(s)")


def add_sub_account(uid, password, name=""):
    uid = str(uid).strip()
    if uid in sub_accounts:
        return False, f"الحساب {uid} موجود مسبقاً"
    sub_accounts[uid] = SubAccount(uid, password, name)
    save_sub_accounts()
    return True, f"تم إضافة الحساب {name or uid}"


def remove_sub_account(uid):
    uid = str(uid).strip()
    if uid not in sub_accounts:
        return False, "الحساب غير موجود"
    sub = sub_accounts.pop(uid)
    for t in sub.tasks:
        t.cancel()
    if uid in sub_spam_tasks:
        sub_spam_tasks[uid].cancel()
        del sub_spam_tasks[uid]
    save_sub_accounts()
    return True, f"تم حذف الحساب {sub.name}"


async def login_sub_account(sub):
    try:
        open_id, access_token = await GeNeRaTeAccEss(sub.uid, sub.password)
        if not open_id or not access_token:
            print(f"[SUB {sub.name}] فشل تسجيل الدخول")
            return False

        platform_id = 2
        PyL = await EncRypTMajoRLoGin(open_id, access_token, platform_id)
        resp = await MajorLogin(PyL)
        if not resp:
            PyL = await EncRypTMajoRLoGin(open_id, access_token, platform_id)
            resp = await MajorLogin(PyL)
            if not resp:
                print(f"[SUB {sub.name}] الحساب محظور أو غير موجود")
                return False

        auth = await DecRypTMajoRLoGin(resp)
        sub.token = auth.token
        sub.key = auth.key
        sub.iv = auth.iv
        sub.region = auth.region
        sub.bot_uid = auth.account_uid
        sub.timestamp = auth.timestamp
        sub.url = auth.url

        login_data = await GetLoginData(auth.url, PyL, sub.token)
        if not login_data:
            print(f"[SUB {sub.name}] فشل الحصول على البيانات")
            return False

        parsed = await DecRypTLoGinDaTa(login_data)
        online_ports = parsed.Online_IP_Port
        chat_ports = parsed.AccountIP_Port
        if ':' not in online_ports: online_ports = "0.0.0.0:0"
        if ':' not in chat_ports: chat_ports = "0.0.0.0:0"

        sub.online_ip, sub.online_port = online_ports.split(":")
        sub.chat_ip, sub.chat_port = chat_ports.split(":")

        sub.chat_auth = create_auth_token_chat(
            int(sub.bot_uid), sub.token, int(sub.timestamp), sub.key, sub.iv
        )
        sub.online_auth, _ = build_ob55_online_auth(
            int(sub.bot_uid), sub.token, int(sub.timestamp), sub.key, sub.iv
        )

        sub.connected = True
        print(f"[SUB {sub.name}] ✓ متصل | UID={sub.bot_uid} | region={sub.region}")
        return True

    except Exception as e:
        print(f"[SUB {sub.name}] Login error: {e}")
        return False


async def sub_online_worker(sub, reconnect_delay=2.0):
    while True:
        try:
            reader, writer = await asyncio.open_connection(sub.online_ip, int(sub.online_port))
            sub.online_writer = writer
            token_bytes = bytes.fromhex(sub.online_auth) if isinstance(sub.online_auth, str) else sub.online_auth
            writer.write(token_bytes)
            await writer.drain()
            print(f"[SUB {sub.name}] Online TCP connected")

            while True:
                data = await reader.read(9999)
                if not data:
                    break
                await asyncio.sleep(0)

            writer.close()
            try: await writer.wait_closed()
            except: pass
            sub.online_writer = None
        except Exception as e:
            print(f"[SUB {sub.name}] Online error: {e}")
            sub.online_writer = None
        await asyncio.sleep(reconnect_delay)


async def sub_chat_worker(sub, reconnect_delay=2.0):
    while True:
        try:
            reader, writer = await asyncio.open_connection(sub.chat_ip, int(sub.chat_port))
            sub.whisper_writer = writer
            payload = bytes.fromhex(sub.chat_auth) if isinstance(sub.chat_auth, str) else sub.chat_auth
            writer.write(payload)
            await writer.drain()
            sub.ready_event.set()
            print(f"[SUB {sub.name}] Chat TCP connected")

            while True:
                data = await reader.read(9999)
                if not data:
                    break
                await asyncio.sleep(0)

            writer.close()
            try: await writer.wait_closed()
            except: pass
            sub.whisper_writer = None
        except Exception as e:
            print(f"[SUB {sub.name}] Chat error: {e}")
            sub.whisper_writer = None
        await asyncio.sleep(reconnect_delay)


async def start_sub_account(uid):
    sub = sub_accounts.get(uid)
    if not sub:
        return False, "الحساب غير موجود"
    if sub.connected:
        return True, "الحساب متصل مسبقاً"

    ok = await login_sub_account(sub)
    if not ok:
        return False, "فشل تسجيل الدخول"

    t1 = asyncio.create_task(sub_online_worker(sub))
    t2 = asyncio.create_task(sub_chat_worker(sub))
    sub.tasks = [t1, t2]

    try:
        await asyncio.wait_for(sub.ready_event.wait(), timeout=10)
    except asyncio.TimeoutError:
        pass

    return True, f"تم تشغيل {sub.name}"


async def stop_sub_account(uid):
    sub = sub_accounts.get(uid)
    if not sub:
        return False, "الحساب غير موجود"
    for t in sub.tasks:
        t.cancel()
    sub.tasks = []
    if sub.online_writer:
        try: sub.online_writer.close()
        except: pass
    if sub.whisper_writer:
        try: sub.whisper_writer.close()
        except: pass
    sub.online_writer = None
    sub.whisper_writer = None
    sub.connected = False
    sub.ready_event = asyncio.Event()
    return True, f"تم إيقاف {sub.name}"


async def sub_spam_squad_loop(sub, team_code, count=30, delay=0.25):
    while True:
        try:
            if not sub.online_writer:
                await asyncio.sleep(1)
                continue

            try:
                join_pkt = await JoinSq_original(team_code, sub.key, sub.iv)
                sub.online_writer.write(join_pkt)
                await sub.online_writer.drain()
            except Exception as e:
                print(f"[SUB {sub.name}] join error: {e}")
                await asyncio.sleep(1)
                continue

            await asyncio.sleep(0.35)

            for _ in range(count):
                try:
                    lag_pkt = await xSKINZxLag(sub.key, sub.iv)
                    sub.online_writer.write(lag_pkt)
                    await sub.online_writer.drain()
                    await asyncio.sleep(delay)
                except Exception:
                    break

            try:
                leave_pkt = await ExiT(None, sub.key, sub.iv)
                sub.online_writer.write(leave_pkt)
                await sub.online_writer.drain()
            except Exception:
                pass

            await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            print(f"[SUB {sub.name}] spam loop cancelled")
            break
        except Exception as e:
            print(f"[SUB {sub.name}] spam loop error: {e}")
            await asyncio.sleep(1)


async def start_multi_squad_spam(team_code, count_per_sub=30, delay=0.25):
    global active_sub_spam
    active_sub_spam = True
    results = []
    for uid, sub in sub_accounts.items():
        if not sub.connected:
            results.append(f"❌ {sub.name} غير متصل")
            continue
        if uid in sub_spam_tasks and not sub_spam_tasks[uid].done():
            results.append(f"⚠ {sub.name} يعمل مسبقاً")
            continue
        task = asyncio.create_task(sub_spam_squad_loop(sub, team_code, count_per_sub, delay))
        sub_spam_tasks[uid] = task
        results.append(f"✓ {sub.name} بدأ")
    return results


async def stop_multi_squad_spam():
    global active_sub_spam
    active_sub_spam = False
    for uid, task in list(sub_spam_tasks.items()):
        task.cancel()
        del sub_spam_tasks[uid]
    return True


async def handle_sub_command(inPuTMsG, uid, chat_id, chat_type, key, iv):
    """
    معالج أوامر الحسابات الفرعية — يرجع True إذا تم التعامل مع الأمر.
    """
    msg = inPuTMsG.strip()

    if msg.startswith("/sub add "):
        parts = msg.split()
        if len(parts) < 4:
            reply = f"[B][C]{get_random_color()} Usage: /sub add <uid> <password> [name]"
        else:
            sub_uid = parts[2]
            sub_pass = parts[3]
            sub_name = parts[4] if len(parts) >= 5 else ""
            ok, txt = add_sub_account(sub_uid, sub_pass, sub_name)
            reply = f"[B][C]{get_random_color()} {'✓' if ok else '✗'} {txt}"
        P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
        return True

    if msg.startswith("/sub remove "):
        parts = msg.split()
        if len(parts) < 3:
            reply = f"[B][C]{get_random_color()} Usage: /sub remove <uid>"
        else:
            ok, txt = remove_sub_account(parts[2])
            reply = f"[B][C]{get_random_color()} {'✓' if ok else '✗'} {txt}"
        P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
        return True

    if msg == "/sub list":
        if not sub_accounts:
            reply = f"[B][C]{get_random_color()} لا يوجد حسابات فرعية"
        else:
            lines = [f"[B][C]{get_random_color()} الحسابات الفرعية:"]
            for s in sub_accounts.values():
                status = "🟢 متصل" if s.connected else "🔴 غير متصل"
                running = "⚡ يعمل" if s.uid in sub_spam_tasks and not sub_spam_tasks[s.uid].done() else ""
                lines.append(f"[B][C][FFFFFF]• {s.name} | UID={s.uid} | {status} {running}")
            reply = "\n".join(lines)
        P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
        return True

    if msg.startswith("/sub start "):
        parts = msg.split()
        if len(parts) < 3:
            reply = f"[B][C]{get_random_color()} Usage: /sub start <uid>"
        else:
            ok, txt = await start_sub_account(parts[2])
            reply = f"[B][C]{get_random_color()} {'✓' if ok else '✗'} {txt}"
        P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
        return True

    if msg.startswith("/sub stop "):
        parts = msg.split()
        if len(parts) < 3:
            reply = f"[B][C]{get_random_color()} Usage: /sub stop <uid>"
        else:
            ok, txt = await stop_sub_account(parts[2])
            reply = f"[B][C]{get_random_color()} {'✓' if ok else '✗'} {txt}"
        P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
        return True

    if msg == "/sub startall":
        started = 0
        for s_uid in list(sub_accounts.keys()):
            ok, _ = await start_sub_account(s_uid)
            if ok: started += 1
            await asyncio.sleep(3)
        reply = f"[B][C]{get_random_color()} ✓ تم تشغيل {started} حساب"
        P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
        return True

    if msg == "/sub stopall":
        for s_uid in list(sub_accounts.keys()):
            await stop_sub_account(s_uid)
        await stop_multi_squad_spam()
        reply = f"[B][C]{get_random_color()} ✓ تم إيقاف جميع الحسابات"
        P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
        return True

    if msg.startswith("/squadspam "):
        parts = msg.split()
        if len(parts) < 2:
            reply = f"[B][C]{get_random_color()} Usage: /squadspam <teamcode> [count] [delay]"
            P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
            return True

        team_code = parts[1]
        count = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 30
        delay = float(parts[3]) if len(parts) >= 4 else 0.25

        for s_uid, s in sub_accounts.items():
            if not s.connected:
                await start_sub_account(s_uid)
                await asyncio.sleep(2)

        results = await start_multi_squad_spam(team_code, count, delay)
        reply = f"[B][C]{get_random_color()} صباب الفريق {team_code}:\n" + "\n".join(
            f"[B][C][FFFFFF]{r}" for r in results
        )
        P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
        return True

    if msg == "/squadstop":
        await stop_multi_squad_spam()
        reply = f"[B][C]{get_random_color()} ✓ تم إيقاف صباب الفريق"
        P = await SEndMsG(chat_type, reply, uid, chat_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
        return True

    return False


async def auto_start_all_subs():
    load_sub_accounts()
    if not sub_accounts:
        return
    print(f"[SUB] Auto-starting {len(sub_accounts)} sub account(s)...")
    for s_uid in list(sub_accounts.keys()):
        ok, msg = await start_sub_account(s_uid)
        print(f"[SUB] {msg}")
        await asyncio.sleep(3)

# ==================================================================
#   END SUB ACCOUNTS
# ==================================================================


async def accept_squad_invite(group_id, joiner_account_id, invitee_type, key, iv, region="bd"):
    fields = {
        1: 38,
        2: {
            1: int(group_id),
            2: int(joiner_account_id),
            8: int(invitee_type)
        }
    }
    proto_hex = (await CrEaTe_ProTo(fields)).hex()
    if region.lower() == "ind":
        pkt_type = '0514'
    elif region.lower() == "bd":
        pkt_type = '0519'
    else:
        pkt_type = '0515'
    return await GeneRaTePk(proto_hex, pkt_type, key, iv)


async def create_lw5_packet1(key, iv):
    fields = {
        1: 1,
        2: {
            2: "0b",
            3: 43,
            4: 1,
            5: "en",
            8: {
                1: "IDC3",
                2: 109,
                3: "BD"
            },
            9: 1,
            10: "01090a0b12192027",
            11: 1,
            13: 1,
            14: {
                1: "08FBA33CFF11B915020FEB099999000002BF000C02B3024798E574700E9B7471467625142200004375a63c900e748c3f6a311e04000000ff42002b09cacfa16d",
                2: 1023,
                3: "715c5f561b06054a03530500570555590f5100040250000255010e520653000303525306570c02501500064d72584347411f051a001e1206074f184a677f5a564775765b554a6d74451d58584902574440625b64670c14004b600a7d4d740c670100040264586b7d586c02016e657c5b60574b03081500044d60795258726206047f04600f405b18710347746242085f055442530a160e4d58496f0c7c4501750763605c575f02765a485e5f4c407f4b625e400e1b054973535b40655e5151566e6507797e6b47727a1c6e455762637d650708100a447e704c1e530200437a604b66065d595f58444c5a06095a6c44677a0c14040b4f705b7d75624374067840416d60067353017f5972506356014354680a12044950406940046265725c4f5d56416250737b7c42035b7d43667f645e0b12034d5262577f13554f574b0e76580201766b50520347756947616747000a1604014b6473441869176f795f5d78755c58057d4d545e53747b08725274580514004a08666b585654050b5c7f5e584d725a4f69755056055c61475566740e100f4b76514178587d5f727f00077a070953507f72467d40794f667b42560a16054f7a627d4778417a047c7e717b7c4c78557b4b7807627a01714573600a",
                4: "{Y_R",
                6: 13,
                7: "136601706b6f54001513",
                8: "2.126.20",
                9: 2,
                10: 1,
                11: "0362625351367a6e2f56386835416456324b796f566c576a3265735033734558335070623443526c656c47355732327673496e477a425a49677375684a664336766942532f4277526d3632374a624a724c562b586b664c516c6749424634615664544e34473869494c456e4c7643347754354670754e66444b7746367765466d675a2b2b36704d57694549677343415756457144554a454d737271515a306b544266635a45334959485661774d5332622b4f6b5a477a755171576e3645624c6a3932646c31307754614d55714e355556796742506f454572376651673249754332545a773333796c71493838627562512f6e425072353577785436617955544d63624d2b756d4542345a736c5165504d58725365737864622f30577967786849384a352f73484f685730494a49643450534b6d6a41734b315251495257634c35646355554b457a6d33334475735877384a5267483364412b66414b336433726a4241317954327451326c2f73584c6d4b4b6456707062624b4f3272694c384947382b634a3863506533314241456d517876744f495631323658394c4b35414c6a38533631526d6159617077676e4579724a5a7a624d4c596c3542554b6d7934473546334c6152454c4e6d726a684f557065462f39686f64356c526378623250777244566d455665715355764a456548644e375279344c707035563466392f6f6d3837426b5a6c6159364c5439396b5579554266434c5538647a4a2b43655a35784a44585064654a51686e46496945506e7a506d32334b637a2b512b384a51307a522b5a5a41762b694873454748516a536e61467862356a517a526c374f5a793241385731437263513d"
            },
            19: 329,
            21: "374f5219",
            24: {
                1: 21
            },
            27: "a_1477547277882450725"
        }
    }
    proto_data = await CrEaTe_ProTo(fields)
    return await GeneRaTePk(proto_data.hex(), '0515', key, iv)


async def create_lw5_packet2(key, iv):
    fields = {
        1: 17,
        2: {
            1: 17165783357,
            2: 1,
            3: 4,
            4: 6100,
            5: b'\x1a',
            8: 5,
            13: 329
        }
    }
    proto_data = await CrEaTe_ProTo(fields)
    return await GeneRaTePk(proto_data.hex(), '0515', key, iv)


async def leave_channel_req(channel_id, channel_type, key, iv):
    fields = {
        1: 4,
        2: {
            1: int(channel_id),
            2: int(channel_type),
            3: "en"
        }
    }
    proto_hex = (await CrEaTe_ProTo(fields)).hex()
    return await GeneRaTePk(proto_hex, '1215', key, iv)


async def MahirAccepted(uid, code, K, V, region):
    fields = {
        1: 4,
        2: {
            1: int(uid),
            3: int(uid),
            4: "rYUW\u0017\t\u0007NR\u000f\u0005\u0004\\W\u0002\u000fV\u0004TPQ\u0003\u000f\u0005\u0004]\u0005\u000b\u0000\u0002W\u0005\r\nVY\u0004\u0007\u0007\u0007\u0017\u0001\bNQ\fS\u0005\u000e\u0006\f\u0005\rT\u0002\r\u0000TS\u0007U\u0004X\u0006RY\u0005\u0003\u0003SW\u0000\u000eQ\u0000^\u0014\u0003\u0004JXS\u000f\u0000g{gqfePblEZJp{_xU\bmQW\f\u0007\u000f\u0015\u0007H\\U^CHXh@Ax`\u000f~\rPN~Nz\\\u001fr_ckPU\u000b\u0015\u0005\u0003Ekfp\u007fcY\u0001nWV\u0002\u0005azE~N\u0007HTg\u0007w}@\u0002{\t\u0013\u0001NeNP]DUf|]ugINs{\u0001\\rAgB_\u0004fA]\u0004\r\u001a\u0000HpOv\\\u0005|gS\u0019\\A\u007fWW\u0003\u007fTqFGi\u0004Qr\u0019dW\u0004\u0011\rD\u0002yeZ]A\u0016a\u0007Ungeg\u0007E\\P\u001b\u000b\u0004\u0005dd\ff\r\u000f\u0017\t\u000fNRyZd\u0012tZ\fe\u0007AGv\u007fw]{P\u001c\u007f\u000ff\u0005ZJK\u0004\u0005\u0014\u0001Jgq\u001frymsB\u0001\u000fd`\u001bj~[}\u0004SDZ|IxKB\u0000\n\u0011\u0002J^`tjYN\u0007WQdE[PQ\u0002_{VP`S\t\u007f\u001dP\n\u000f\u000f\u0015\u0004\u0004L{V\u0003@yP[rgJ{Hpk\u0001\u000bw\u0001Agt_\u0003HxPc\u000b\u0017\u0005E`y\ngWoy\u0006S{A\u001b``Y\u0002Xzw}dDWOKsg\t\u0013\u000eNf\u000e\\`\u0005AwJc\u0003BM\u0001^\u0000R\u0002Qoh]tbS~`G\r\u001a\u0004HibAQ\u000f^XNt~kDUWaeaNqcLQ]Py[G\u0004",
            8: 1,
            9: {
                1: "08FBA33CFF105FDD02030A01111100000003000100020000B687046B0D6D2083467625141101030175a63c900e748c3f6a311e04000000ff00040202cacfa16d",
                2: 197,
                3: "735154531406054a035305025a0e50560f5100040250020f5e0401520653000303505e0d520302501500064d705548424e1f051a001e12040a441d45677f5a56477574565e4f6274451d585849005a4f456d5b64670c1400496d017842740c670100040069536e72586c02016e657e566b524403081500044d0d515b637f757c19095c7c510b00417f565e5e066f5b584d6e02400a160e4d58496d0177400e750763605c575d0f7d5f475e5f4c407f4b60534b0b140549751e66407a4860726e44400e6103557e515c646f43577106077b60051b0f4b4e474e58457e4f615c01406000051e0361516e7b67614e040102540c1609004a0e40730f62517f446a135179507e706850547f7e7a67795553597c0a1206445b456640046265725c4d505d446d50737b7c42035970486370645e0b12034d506f5c7a1c554f574b0e765a0f0a7364505203477569456c6c420f0a1604014b647149136c186f795f5d78755e550e7842545e53747b08705f7f5d0a14004a08666b5a5b5f00045c7f5e584d72584262705f56055c61475564790515004b76514178587f52797a0f077a070953507d7f4d784f794f667b4256081b0e4a75627d4778417a06717574747c4c78557b4b7a0a697f0e714573600a",
                4: "{Y_R",
                6: 13,
                7: "1c6f017306741513",
                8: "2.126.22",
                9: 2,
                10: 1,
                11: "036262535136784d584e38742b416456324b796f566c6537392f763672324d4773683377706379634f6b6b6f7447397934523930517432797246474c464c624c777a7747344d4d5257623146756d2b6556777a62597736743973492b787059612f55596e43465a59485235713348705830306c485a6f3632565145574e7145705866414c38734a754646575247386d376f5444566a4c484c576e714c52576f4e653236437a734c346a4144482b71694b55384d71476c5936546669544863716c487a345568463869396a365233366d49774b71746c36714f797568513854367945714b69742f45466d2b3636634b787649304f384d6c41385166454b6253467366477a2f30684e6a543873305369722f3962635a2f4679376c752f61632f366765704d415734566b442b36374f535756473665377a775a79312b797a42334b70632b7571782b577437414d39727761644439575765704570567849526d66654f5847627775787a68574b5664344c49586f415a364b387063647450627953575857455135706a542b653669365a2f6177444a3179356f774d5a534f64487262392b7877424a4d444f7848565a7a483372547a30314a316738517675466b656c3042433746473630683456427041494457487956684e794b4f5145507139467a4a6272686b316146306768766578512f6a2f484767426a5268675134645243372b477a5a687a2b77505967376454746f2b494d56363774642b54435653314234346b4148395a6d415872763758615941626477644e664f533170484875736b7336346f72463452534d3772656345417661484e61484854314c5545686e786a5a616d4d4f303d"
            },
            10: str(code),
            11: {
                1: "IDC3",
                2: 84,
                3: "BD"
            },
            13: "en",
            16: "374f5219",
            20: {
                1: 21
            },
            23: "a_1477547277882450725",
            27: {
                1: 2,
                2: 8
            }
        }
    }
    proto_data = await CrEaTe_ProTo(fields)
    return await GeneRaTePk(proto_data.hex(), '0515', K, V)


def load_spam_targets():
    if os.path.exists(SPAM_TARGETS_FILE):
        with open(SPAM_TARGETS_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_spam_targets():
    with open(SPAM_TARGETS_FILE, "w") as f:
        json.dump(list(spam_targets), f)


online_writer = None
whisper_writer = None
spam_room = False
spammer_uid = None
spam_chat_id = None
spam_uid = None
Spy = False
Chat_Leave = False
evo_cycle_running = False
evo_cycle_task = None

spam_targets = load_spam_targets()
spam_task = None
spam_running = False
SPAM_INTERVAL = 0.5

bot_uid = None

pending_ghost_future = None
ghost_team_id = None
ghost_squad_code = None

spm_tasks = {}

# Ban variables
ban_task = None
ban_running = False
ban_targets = set()

EVO_IDS = [
    909000063, 909000075, 909040010, 909000081, 909039011,
    909045001, 909038012, 909042008, 909051003, 909035012,
    909000098, 909033002, 909037011, 909049010, 909041005,
    909038010, 909033001, 909035007, 909000090, 909000085,
    909000068
]

LIKE_LIMIT_FILE = "like_limit.json"
if os.path.exists(LIKE_LIMIT_FILE):
    with open(LIKE_LIMIT_FILE, "r") as f:
        like_limit = json.load(f)
else:
    like_limit = {}

admin_uid = "8173310382"
ToKen = None
region = None

Hr = {
    "User-Agent": "UnityPlayer/2018.4.12f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
    "Accept": "*/*",
    "Accept-Encoding": "deflate, gzip",
    "X-Ga-Sv": "1789534056",
    "Authorization": "Bearer",
    "X-Ga": "v1 1",
    "Releaseversion": "OB55",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Unity-Version": "2018.4.12f1"
}


def get_random_color():
    colors = [
        "[FF0000]", "[00FF00]", "[0000FF]", "[FFFF00]", "[FF00FF]", "[00FFFF]", "[FFFFFF]", "[FFA500]",
        "[A52A2A]", "[800080]", "[000000]", "[808080]", "[C0C0C0]", "[FFC0CB]", "[FFD700]", "[ADD8E6]",
        "[90EE90]", "[D2691E]", "[DC143C]", "[00CED1]", "[9400D3]", "[F08080]", "[20B2AA]", "[FF1493]",
        "[7CFC00]", "[B22222]", "[FF4500]", "[DAA520]", "[00BFFF]", "[00FF7F]", "[4682B4]", "[6495ED]",
        "[5F9EA0]", "[DDA0DD]", "[E6E6FA]", "[B0C4DE]", "[556B2F]", "[8FBC8F]", "[2E8B57]", "[3CB371]",
        "[6B8E23]", "[808000]", "[B8860B]", "[CD5C5C]", "[8B0000]", "[FF6347]", "[FF8C00]", "[BDB76B]",
        "[9932CC]", "[8A2BE2]", "[4B0082]", "[6A5ACD]", "[7B68EE]", "[4169E1]", "[1E90FF]", "[191970]",
        "[00008B]", "[000080]", "[008080]", "[008B8B]", "[B0E0E6]", "[AFEEEE]", "[E0FFFF]", "[F5F5DC]",
        "[FAEBD7]"
    ]
    return random.choice(colors)


async def send_friend_request(target_uid, token, region):
    try:
        encrypted_id = Encrypt_ID(target_uid)
        payload = f"08a7c4839f1e10{encrypted_id}1801"

        plain_text = bytes.fromhex(payload)
        key_bytes = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
        iv_bytes = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])
        cipher = AES.new(key_bytes, AES.MODE_CBC, iv_bytes)
        encrypted_payload = cipher.encrypt(pad(plain_text, AES.block_size)).hex()

        url = "https://clientbp.ggpolarbear.com/RequestAddingFriend"
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Unity-Version": "2018.4.11f1",
            "X-GA": "v1 1",
            "ReleaseVersion": "OB55",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Dalvik/2.1.0 (Linux; Android 9)"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=bytes.fromhex(encrypted_payload), ssl=False) as response:
                if response.status == 200:
                    return True, "DoNe SenD"
                else:
                    return False, f"فشل: {response.status}"
    except Exception as e:
        return False, f"خطأ: {str(e)}"


async def remove_friend(target_uid, token, region):
    try:
        msg = RemoveFriend_Req_pb2.RemoveFriend()
        msg.AuthorUid = int(admin_uid)
        msg.TargetUid = int(target_uid)

        proto_bytes = msg.SerializeToString()

        key_bytes = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
        iv_bytes = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])
        cipher = AES.new(key_bytes, AES.MODE_CBC, iv_bytes)
        encrypted_bytes = cipher.encrypt(pad(proto_bytes, AES.block_size))

        url = "https://clientbp.ggpolarbear.com/RemoveFriend"
        headers = {
            'Authorization': f"Bearer {token}",
            'User-Agent': "Dalvik/2.1.0 (Linux; Android 9)",
            'Content-Type': "application/x-www-form-urlencoded",
            'X-Unity-Version': "2018.4.11f1",
            'X-GA': "v1 1",
            'ReleaseVersion': "OB55"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=encrypted_bytes, headers=headers, ssl=False) as response:
                if response.status == 200:
                    return True, "DoNe ReMoVe"
                else:
                    return False, f"فشل: {response.status}"
    except Exception as e:
        return False, f"خطأ: {str(e)}"


async def encrypted_proto(encoded_hex):
global MajoRLoGinauThKey, MajoRLoGinauThIv
MajoRLoGinauThKey = MajoRLoGinauTh.key
MajoRLoGinauThIv  = MajoRLoGinauTh.iv
key = MajoRLoGinauThKey
iv  = MajoRLoGinauThIv
    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded_message = pad(encoded_hex, AES.block_size)
    encrypted_payload = cipher.encrypt(padded_message)
    return encrypted_payload


async def GeNeRaTeAccEss(uid, password):
    url = "https://100067.connect.garena.com/oauth/guest/token/grant"
    headers = {
        "Host": "100067.connect.garena.com",
        "User-Agent": (await Ua()),
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close"}
    data = {
        "uid": uid,
        "password": password,
        "response_type": "token",
        "client_type": "2",
        "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
        "client_id": "100067"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=Hr, data=data) as response:
            if response.status != 200: return "Failed to get access token"
            data = await response.json()
            open_id = data.get("open_id")
            access_token = data.get("access_token")
            return (open_id, access_token) if open_id and access_token else (None, None)


async def InspectToken(access_token):
    url = f"https://100067.connect.garena.com/oauth/token/inspect?token={access_token}"
    headers = {
        "Connection": "close",
        "Host": "100067.connect.garena.com",
        "User-Agent": "GarenaMSDK/4.0.19P4(G011A ;Android 9;en;US;)"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                if 'error' in data:
                    return None
                return int(data.get('platform', 2))
    except:
        return None


async def EncRypTMajoRLoGin(open_id, access_token, platform_id):
    major_login = MajoRLoGinrEq_pb2.MajorLogin()
    major_login.event_time = str(datetime.now())[:-7]
    major_login.game_name = "free fire"
    major_login.platform_id = platform_id
    major_login.client_version = "1.132.1"
    major_login.system_software = "Android OS 9 / API-28 (PQ3B.190801.10101846/G9650ZHU2ARC6)"
    major_login.system_hardware = "Handheld"
    major_login.telecom_operator = "Verizon"
    major_login.network_type = "WIFI"
    major_login.screen_width = 1920
    major_login.screen_height = 1080
    major_login.screen_dpi = "280"
    major_login.processor_details = "ARM64 FP ASIMD AES VMH | 2865 | 4"
    major_login.memory = 3003
    major_login.gpu_renderer = "Adreno (TM) 640"
    major_login.gpu_version = "OpenGL ES 3.1 v1.46"
    major_login.unique_device_id = "Google|34a7dcdf-a7d5-4cb6-8d7e-3b0e448a0c57"
    major_login.client_ip = "223.191.51.89"
    major_login.language = "en"
    major_login.open_id = open_id
    major_login.open_id_type = "4"
    major_login.device_type = "Handheld"
    memory_available = major_login.memory_available
    memory_available.version = 55
    memory_available.hidden_value = 81
    major_login.access_token = access_token
    major_login.platform_sdk_id = 1
    major_login.network_operator_a = "Verizon"
    major_login.network_type_a = "WIFI"
    major_login.client_using_version = "7428b253defc164018c604a1ebbfebdf"
    major_login.external_storage_total = 36235
    major_login.external_storage_available = 31335
    major_login.internal_storage_total = 2519
    major_login.internal_storage_available = 703
    major_login.game_disk_storage_available = 25010
    major_login.game_disk_storage_total = 26628
    major_login.external_sdcard_avail_storage = 32992
    major_login.external_sdcard_total_storage = 36235
    major_login.login_by = 3
    major_login.library_path = "/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/lib/arm64"
    major_login.reg_avatar = 1
    major_login.library_token = "5b892aaabd688e571f688053118a162b|/data/app/com.dts.freefireth-YPKM8jHEwAJlhpmhDhv5MQ==/base.apk"
    major_login.channel_type = 3
    major_login.cpu_type = 2
    major_login.cpu_architecture = "64"
    major_login.client_version_code = "2019116753"
    major_login.graphics_api = "OpenGLES2"
    major_login.supported_astc_bitset = 16383
    major_login.login_open_id_type = 4
    major_login.analytics_detail = b"FwQVTgUPX1UaUllDDwcWCRBpWAUOUgsvA1snWlBaO1kFYg=="
    major_login.loading_time = 13564
    major_login.release_channel = "android"
    major_login.extra_info = "KqsHTymw5/5GB23YGniUYN2/q47GATrq7eFeRatf0NkwLKEMQ0PK5BKEk72dPflAxUlEBir6Vtey83XqF593qsl8hwY="
    major_login.android_engine_init_flag = 110009
    major_login.if_push = 1
    major_login.is_vpn = 1
    major_login.origin_platform_type = "4"
    major_login.primary_platform_type = "4"
    string = major_login.SerializeToString()
    return await encrypted_proto(string)


async def MajorLogin(payload):
    url = "https://loginbp.ggpolarbear.com/MajorLogin"
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload, headers=Hr, ssl=ssl_context) as response:
            if response.status == 200: return await response.read()
            return None


async def GetLoginData(base_url, payload, token):
    url = f"{base_url}/GetLoginData"
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    Hr['Authorization'] = f"Bearer {token}"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload, headers=Hr, ssl=ssl_context) as response:
            if response.status == 200: return await response.read()
            return None


async def DecRypTMajoRLoGin(MajoRLoGinResPonsE):
    class ParsedMajorLogin:
        def __init__(self, fields):
            self.account_uid = int(fields.get(1, 0))
            self.region = str(fields.get(2, ""))
            self.token = str(fields.get(8, ""))
            self.url = str(fields.get(10, ""))
            self.timestamp = int(fields.get(21, 0))
            raw_key = fields.get(22, b"")
            if isinstance(raw_key, str):
                try: self.key = bytes.fromhex(raw_key.replace(" ", ""))
                except: self.key = raw_key.encode()
            else:
                self.key = bytes(raw_key)
            raw_iv = fields.get(23, b"")
            if isinstance(raw_iv, str):
                try: self.iv = bytes.fromhex(raw_iv.replace(" ", ""))
                except: self.iv = raw_iv.encode()
            else:
                self.iv = bytes(raw_iv)

    def _dec_vr(buf, off):
        res, sh, st = 0, 0, off
        while True:
            if off >= len(buf): raise IndexError("Out of bound")
            b = buf[off]; off += 1
            res += (b & 0x7F) << sh; sh += 7
            if b < 0x80: break
        return res, off - st

    def _dec_proto(buf):
        off, parts, blen = 0, [], len(buf)
        try:
            while off < blen:
                it, vl = _dec_vr(buf, off); off += vl
                wt, fn = it & 7, it >> 3
                if wt == 0: val, vl = _dec_vr(buf, off); off += vl
                elif wt == 2:
                    length, vl = _dec_vr(buf, off); off += vl
                    if off + length > blen: return None
                    val = buf[off:off + length]; off += length
                elif wt == 5:
                    if off + 4 > blen: return None
                    val = struct.unpack_from("<I", buf, off)[0]; off += 4
                elif wt == 1:
                    if off + 8 > blen: return None
                    val = struct.unpack_from("<Q", buf, off)[0]; off += 8
                else: return None
                parts.append((fn, wt, val))
            return parts
        except: return None

    def _to_dict(parts):
        res = {}
        for fn, wt, v in parts:
            if wt == 2:
                try: val = v.decode("utf-8")
                except: val = v
            else: val = v
            res[fn] = val
        return res

    buf = MajoRLoGinResPonsE
    try:
        c = AES.new(b'Yg&tc%DEuh6%Zc^8', AES.MODE_CBC, b'6oyZDr22E3ychjM%')
        buf = unpad(c.decrypt(buf), AES.block_size)
    except:
        pass

    parsed = _dec_proto(buf)
    if parsed: return ParsedMajorLogin(_to_dict(parsed))

    for o in range(len(buf)):
        parsed = _dec_proto(buf[o:])
        if parsed:
            d = _to_dict(parsed)
            if 1 in d or 8 in d or 10 in d:
                return ParsedMajorLogin(d)

    try:
        proto = MajoRLoGinrEs_pb2.MajorLoginRes()
        proto.ParseFromString(MajoRLoGinResPonsE)
        return proto
    except:
        pass
    return ParsedMajorLogin({})


async def DecRypTLoGinDaTa(LoGinDaTa):
    proto = PorTs_pb2.GetLoginData()
    proto.ParseFromString(LoGinDaTa)
    return proto


async def DecodeWhisperMessage(hex_packet):
    packet = bytes.fromhex(hex_packet)
    proto = DEcwHisPErMsG_pb2.DecodeWhisper()
    proto.ParseFromString(packet)
    return proto


async def decode_team_packet(hex_packet):
    packet = bytes.fromhex(hex_packet)
    proto = sQ_pb2.recieved_chat()
    proto.ParseFromString(packet)
    return proto


def build_ob55_online_auth(account_uid, token, kts, key, iv):
    token_bytes = token.encode("utf-8")
    cipher = AES.new(key, AES.MODE_CBC, iv)
    enc_token = cipher.encrypt(pad(token_bytes, 16))
    opcode = bytes.fromhex("830c")
    uid_bytes = int(account_uid).to_bytes(8, byteorder="big")
    kts_bytes = int(kts).to_bytes(4, byteorder="big")
    len_bytes = len(enc_token).to_bytes(8, byteorder="big")
    return opcode + uid_bytes + kts_bytes + len_bytes + enc_token, enc_token


def create_auth_token_chat(account_id, jwt_token, timestamp, key, iv):
    try:
        K = bytes.fromhex(key) if isinstance(key, str) else key
        V = bytes.fromhex(iv) if isinstance(iv, str) else iv

        jwt_bytes = jwt_token.encode('utf-8')
        ct = AES.new(K, AES.MODE_CBC, V).encrypt(
            pad(jwt_bytes, AES.block_size)
        )

        uid_b = int(account_id).to_bytes(8, 'big')
        ts_b = int(timestamp).to_bytes(4, 'big')
        len_b = len(ct).to_bytes(4, 'big')

        header = bytes.fromhex('6d19') + uid_b + ts_b + len_b

        return (header + ct).hex()

    except Exception as e:
        print(f"[AUTH TOKEN ERROR] {e}")
        return None


async def cHTypE(H):
    if not H: return 'Squid'
    elif H == 1: return 'CLan'
    elif H == 2: return 'PrivaTe'


# =============== باكت الكلان الجديد ===============
async def NewClanChatPacket(message_text, key, iv, clan_id=3102281341):
    fields = {
        1: 16801068917,
        2: 18,
        4: 1,
        5: {
            1: 16801068917,
            2: clan_id,
            3: 1,
            4: 'SKINZXKING',
            5: 1785944381,
            7: 6,
            9: {
                1: message_text,
                4: 301,
                8: '♧Darsa•City♧',
                13: '',
                14: {
                    1: 16801068917
                },
                18: {
                    1: 16801068917,
                    3: 1,
                    4: ''
                }
            },
            10: 'ar',
            13: {
                2: 1,
                3: 1
            },
            14: '',
            16: int(time.time() * 1000)
        }
    }
    proto_data = await CrEaTe_ProTo(fields)
    return await GeneRaTePk(proto_data.hex(), '0515', key, iv)


async def SEndMsG(H, message, Uid, chat_id, key, iv):
    TypE = await cHTypE(H)
    if TypE == 'Squid':
        msg_packet = await xSEndMsgsQ(message, chat_id, key, iv)
    elif TypE == 'CLan':
        msg_packet = await NewClanChatPacket(message, key, iv, chat_id)
    elif TypE == 'PrivaTe':
        msg_packet = await xSEndMsg(message, 2, Uid, Uid, key, iv)
    return msg_packet


async def SEndPacKeT(OnLinE, ChaT, TypE, PacKeT):
    if TypE == 'ChaT' and ChaT: whisper_writer.write(PacKeT); await whisper_writer.drain()
    elif TypE == 'OnLine': online_writer.write(PacKeT); await online_writer.drain()
    else: return 'UnsoPorTed TypE ! >> ErrrroR (:():)'





async def evo_gun_cycle(uids, evo_ids_list, key, iv, region):
    global evo_cycle_running, whisper_writer, online_writer
    cycle_count = 0
    while evo_cycle_running:
        cycle_count += 1
        print(f"Bắt đầu vòng lặp Evo Gun số #{cycle_count}")
        for emote_id in evo_ids_list:
            if not evo_cycle_running:
                break
            print(f"Đang gửi Evo Emote ID: {emote_id}")
            for uid_str in uids:
                try:
                    uid_int = int(uid_str)
                    H = await Emote_k(uid_int, int(emote_id), key, iv, region)
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', H)
                except Exception as e:
                    print(f"Lỗi gửi emote {emote_id} tới {uid_str}: {e}")
            if evo_cycle_running:
                for i in range(5):
                    if not evo_cycle_running:
                        break
                    await asyncio.sleep(1)
        if evo_cycle_running:
            print("Đã xong 1 vòng 21 khẩu. Chờ 2 giây rồi lặp lại...")
            await asyncio.sleep(2)
    print("Đã dừng vòng lặp Evo.")


async def SPamSq(Uid, K, V):
    bunner = await xBunnEr()
    fields = {
        1: 33,
        2: {
            1: int(Uid),
            2: 'ME',
            3: 1,
            4: 1,
            7: 330,
            8: 19459,
            9: 100,
            12: 1,
            16: 1,
            17: {2: 94, 6: 11, 8: '1.132.1', 9: 3, 10: 2},
            18: 201,
            23: {2: 1, 3: 1},
            24: bunner,
            26: {},
            28: {}
        }
    }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), '0515', K, V)


async def SPamSqForSpm(Uid, K, V):
    same_value = random.choice([4096, 16384, 8192])
    fields = {
        1: 33,
        2: {
            1: int(Uid),
            2: "ME",
            3: 1,
            4: 1,
            5: bytes([1, 7, 9, 10, 11, 18, 25, 26, 32]),
            6: "iG:[C][B][FF0000] @CH9AYFAX1",
            7: 330,
            8: 1000,
            10: "ME",
            11: bytes([49, 97, 99, 52, 98, 56, 48, 101, 99, 102, 48, 52, 55, 56,
                       97, 52, 52, 50, 48, 51, 98, 102, 56, 102, 97, 99, 54, 49, 50, 48, 102, 53]),
            12: 1,
            13: int(Uid),
            14: {
                1: 2203434355,
                2: 8,
                3: "\u0010\u0015\b\n\u000b\u0013\f\u000f\u0011\u0004\u0007\u0002\u0003\r\u000e\u0012\u0001\u0005\u0006"
            },
            16: 1,
            17: 1,
            18: 312,
            19: 46,
            23: bytes([16, 1, 24, 1]),
            24: await xBunnEr(),
            26: "",
            28: "",
            31: {
                1: 1,
                2: same_value
            },
            32: same_value,
            34: {
                1: int(Uid),
                2: 8,
                3: bytes([15, 6, 21, 8, 10, 11, 19, 12, 17, 4, 14, 20, 7, 2, 1, 5, 16, 3, 13, 18])
            }
        },
        10: "en",
        13: {
            2: 1,
            3: 1
        }
    }
    data = await CrEaTe_ProTo(fields)
    return await GeneRaTePk(data.hex(), '0515', K, V)


async def spm_loop(uid, key, iv, interval=0.5):
    while True:
        try:
            if online_writer is not None:
                packet = await SPamSqForSpm(uid, key, iv)
                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', packet)
            else:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            print(f"spm_loop for {uid} cancelled")
            break
        except Exception as e:
            print(f"spm_loop error for {uid}: {e}")
        await asyncio.sleep(interval)


async def spam_loop(key, iv):
    global spam_running, online_writer, spam_targets
    spam_running = True
    while spam_running:
        if online_writer is not None and spam_targets:
            for uid in list(spam_targets):
                try:
                    packet = await SPamSq(uid, key, iv)
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', packet)
                except Exception as e:
                    print(f"Spam error for {uid}: {e}")
        await asyncio.sleep(SPAM_INTERVAL)
    print("Spam loop stopped")


async def start_spam_if_needed(key, iv):
    global spam_task, spam_running
    if spam_task is None or spam_task.done():
        if spam_running:
            spam_running = False
        spam_task = asyncio.create_task(spam_loop(key, iv))


async def JoinSq_original(code, K, V):
    skinz = {
        1: 4,
        2: {
            4: "\u0001\u0007\t\n\u000b\u0012\u0019 '",
            5: str(code),
            6: 6,
            8: 1,
            9: {
                1: "08FFF3BE903F27DF0203110111110000006B0003006800169194106F13CF106E4676251411010404dfe9e8b5ca3ca4f96a3119c00000004f03060301cacfa16d",
                2: 130,
                3: "tY_S\u0013\b\u0001M\u0002\u0000T\u0000\u0005\u000e\b\t\u0002\u0000\u0003U\u0003\u0001V\u000fR\u000e\tRQ\u0002\u0004\u0005US\u0003XS\u0005\u0001\u0002\u0011\u0001\u0002JuTAEN\u001e\u0002\u001c\u0002\u001f\u0013\b\u0003M\u001cDbz_Q@}p_QOgsC\u001dYVI\u0004UAAj_ga\u0004\u0012\u0000K\u001d\u0007_t\u0019b\bCx\u0002UeGat\u001fTTCBLER`\u0006\u0001\r\f\u0012\u0006\u0005N^^\u0002acH~r\u0004_\u0003RO|\u000bcd_@~`Rnqrgh\n\u0015\nL\\Nh\u0000~G\u000et\u0000eb]VQ\u0006t^F[ZIGxCdZD\u000b\u0011\u0002OA~LP@\u000fcSDyATQApaT^{|wa]\\\u0001E\u000f\u0013\nJfxve}\u0004J\u0003c{YL\u0007^^yDYQf\bhe\u0005_wg\r\u0010\u0007\bEHpX@||\u0002Z\\uTGyA\u0001JP\u0005t~VkTE\u0000J\\\u000b\u0013\nMr\u0018zTzK}qE]vnvwROuheYvwduvQ\r\u001a\u0005M\u0006z\u001dfXfzvHFc~dXUz}O\u001aB\u0005t\u0002i\u001co_\u0004\u0012\u0003\u0007J\u0006b\u0003\u000eqlfvbEstgu~wB\u0001v@`yI\u0002ZPx\f\u0014\u0003NR\u0002yBZvuD_cd\u0004q]lH]\u0007bD}pCe\t\u0004t\n",
                4: "w^_R",
                6: 11,
                7: "\u0014\u0004aqrg\u0015\u0013",
                8: "1.132.1",
                9: 3,
                10: 2,
                11: "\u0003bbSQ6wxegh9qAdV2KyoVleA17yPmYF+yTnOrl+JMknmppeUBe5ZsiHueP2mZZ4KOs6b2Ail1S9z3qIeU9hGFZ2M6PP39/zjIc/RFVPAkgDagySswMVmYaPhkzU5IqNPow2843fyQUz9xI10NdMhl1WiI4Y6wCXBotiUS9wSgujQ4j0fWXUyklCxBWo8r27hyoGSVrPdTPXFMnJJpPRRFFmWqc3fWvMg+BNfxSRJOZRSrkzG0nNvSIJ4uZB2pqAlHIPEYx7bI6zsgwUVDiLZJKTUTyuCGbOd1DegDUFfazFesTG1LJknT5WhgzCsrBIy+f2l+LeJe5DW7wEwNaHambM7ECXcJcLIhB9kJJsW0tFvXkk3HSdcQ8N1K6wjSKWhpkW0kV8Zrjj0jkhS/7AZ6T9GJRIA827YDtTorBvbx1UkXjYrqYI1nSa2GaMexGnqlurc5DE3v1R+mUBI9GqmjEPgTSYVBxyeCdQMHaMXGtspAhvkiO84ToU87sP45pylDEfFOVoc/rcmdzWeqlYPsv6txKRtIHcb0cO+MoVShoU8ZUVRDF3znbqzVrscPIfplBaa79lwvQqzRubLl9XY="
            },
            11: {
                1: "IDC4",
                2: 281,
                3: "ME"
            },
            13: "fr",
            16: "7OR\u0019",
            20: "\b\u0015",
            27: "\bH\u0010\u0003"
        }
    }
    proto_data = await CrEaTe_ProTo(skinz)
    return await GeneRaTePk(proto_data.hex(), '0515', K, V)


async def GhostPakcet(player_id, nm, secret_code, key, iv):
    fields = {
        1: 61,
        2: {
            1: int(player_id),
            2: {
                1: int(player_id),
                2: 1159,
                3: f"[b][c]{get_random_color()}{nm}",
                5: 12,
                6: 999,
                7: 1,
                8: {
                    2: 1,
                    3: 1,
                },
                9: 3,
            },
            3: secret_code,
        },
    }
    proto_data = await CrEaTe_ProTo(fields)
    return await GeneRaTePk(proto_data.hex(), '0515', key, iv)


async def xSKINZxLag(K, I):
    fields = {
        1: 15,
        2: {
            1: 804266360,
            2: 1
        }
    }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), '0515', K, I)


async def get_squad_info(teamcode, key, iv):
    global pending_ghost_future, ghost_team_id, ghost_squad_code
    pending_ghost_future = asyncio.Future()
    join_pkt = JoinSq(teamcode, key, iv)
    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_pkt)
    try:
        await asyncio.wait_for(pending_ghost_future, timeout=8.0)
        return ghost_team_id, ghost_squad_code
    except asyncio.TimeoutError:
        return None, None
    finally:
        pending_ghost_future = None


async def send_spam_messages(team_id, sq, message, key, iv, count=50, delay=0.2):
    open_pkt = OpenCh(team_id, sq, key, iv)
    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', open_pkt)
    await asyncio.sleep(0.5)
    for i in range(count):
        msg_pkt = MsqSq(message, team_id, key, iv)
        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', msg_pkt)
        await asyncio.sleep(delay)
    exit_pkt = ExitSq('000000', key, iv)
    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', exit_pkt)


async def GenJoinSquadsPacket(squad_code, key, iv):
    return await JoinSq_original(squad_code, key, iv)


async def safe_send_message(chat_type, message, uid, chat_id, key, iv):
    try:
        packet = await SEndMsG(chat_type, message, uid, chat_id, key, iv)
        if packet:
            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', packet)
    except Exception as e:
        print(f"safe_send_message error: {e}")


# ==================== FUNCTIONS FOR /check, /info AND /ban ====================

async def check_banned(player_id):
    try:
        url = f"https://ff.garena.com/api/antihack/check_banned?lang=en&uid={player_id}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 10)",
            "Accept": "application/json",
            "referer": "https://ff.garena.com/en/support/",
            "x-requested-with": "B6FksShzIgjfrYImLpTsadjS86sddhFH"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    return True, data
                else:
                    return False, f"Failed: {response.status}"
    except Exception as e:
        return False, f"Error: {str(e)}"


async def get_player_info(player_id):
    try:
        url = f"https://nirob-x-info.vercel.app/info?uid={player_id}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=False) as response:
                if response.status == 200:
                    data = await response.json()
                    return True, data
                else:
                    return False, f"Failed: {response.status}"
    except Exception as e:
        return False, f"Error: {str(e)}"


async def xBaNchaTxSkInZ(uidd, K, V):
    fields = {
        1: 3,
        2: {
            1: int(uidd),
            3: "fr",
            4: "1750728024661459697_3qind8eeqs",
        }
    }
    Pk = await CrEaTe_ProTo(fields)
    return await GeneRaTePk(Pk.hex(), '1215', K, V)


async def ban_loop(key, iv, interval=0.5):
    global ban_running, ban_targets
    ban_running = True
    while ban_running:
        if online_writer is not None and ban_targets:
            for uid in list(ban_targets):
                try:
                    ban_pkt = await xBaNchaTxSkInZ(uid, key, iv)
                    await SEndPacKeT(whisper_writer, online_writer, 'OnLine', ban_pkt)
                    print(f"Ban packet sent to UID: {uid}")
                except Exception as e:
                    print(f"Ban error for {uid}: {e}")
        await asyncio.sleep(interval)
    print("Ban loop stopped")


def format_player_info(data):
    basic = data.get("basicInfo", {})
    profile = data.get("profileInfo", {})
    clan = data.get("clanBasicInfo", {})
    captain = data.get("captainBasicInfo", {})
    pet = data.get("petInfo", {})
    social = data.get("socialInfo", {})
    credit = data.get("creditScoreInfo", {})
    diamond = data.get("diamondCostRes", {})
    dev = data.get("Dev", "")

    def xMsGFixinG(value):
        return str(value) if value else "0"

    msg1 = f"""
[b][c]{get_random_color()} [SuccessFully] - Get PLayer s'InFo !

[b][c][90EE90]Name : {captain.get('nickname', basic.get('nickname', 'Unknown'))}
[b][c][90EE90]Uid : {xMsGFixinG(captain.get('accountId', basic.get('accountId', '0')))}
[b][c][90EE90]Likes : {xMsGFixinG(captain.get('liked', basic.get('liked', '0')))}
[b][c][90EE90]LeveL : {captain.get('level', basic.get('level', '0'))}
[b][c][90EE90]Server : {captain.get('region', basic.get('region', 'Unknown'))}
"""

    msg2 = f"""
[b][c]{get_random_color()} [SuccessFully] - Get PLayer s'InFo !

[b][c][90EE90]Guild : {clan.get('clanName', 'No Guild')}
[b][c][90EE90]Guild Uid : {xMsGFixinG(clan.get('clanId', '0'))}
[b][c][90EE90]Leader : {xMsGFixinG(clan.get('captainId', 'Unknown'))}
[b][c][90EE90]Members : {clan.get('memberNum', '0')}/{clan.get('capacity', '0')}
[b][c][90EE90]Guild Level : {clan.get('clanLevel', '0')}
"""

    msg3 = f"""
[b][c]{get_random_color()} [SuccessFully] - Get PLayer s'InFo !

[b][c][90EE90]Pet Name : {pet.get('name', 'N/A')}
[b][c][90EE90]Pet Level : {pet.get('level', 'N/A')}
[b][c][90EE90]Credit Score : {credit.get('creditScore', 'N/A')}
[b][c][90EE90]Diamond Cost : {diamond.get('diamondCost', 'N/A')}
[b][c][90EE90]Signature : {social.get('signature', 'N/A')}
"""

    return msg1, msg2, msg3


def format_check_result(data):
    try:
        is_banned = data.get('is_banned', False)
        reason = data.get('reason', 'None')
        ban_type = data.get('ban_type', 'Unknown')
        ban_duration = data.get('ban_duration', 'Unknown')
        ban_date = data.get('ban_date', 'Unknown')
        uid = data.get('uid', 'Unknown')

        status = "BANNED" if is_banned else "CLEAN"
        status_color = "[FF0000]" if is_banned else "[00FF00]"

        msg = f"""
[b][c]{get_random_color()} BAN CHECK RESULT

[b][c][FFFFFF]BAN TYPE => {ban_type}
[b][c][FFFFFF]BAN DURATION => {ban_duration}
"""
        return msg
    except Exception as e:
        return f"[B][C]{get_random_color()} ERROR: {str(e)}"


async def GeTSQDaTa(D):
    try:
        uid = D['5']['data']['1']['data']
        chat_code = D['5']['data']['17']['data']
        squad_code = D['5']['data']['31']['data']
        return uid, chat_code, squad_code
    except KeyError:
        return None, None, None


def format_packet(packet, indent=0, show_wire_type=False):
    lines = []
    indent_str = "  " * indent
    keys = sorted(packet.keys(), key=lambda k: int(k) if k.isdigit() else k)
    for key in keys:
        value = packet[key]
        wire_type = value.get("wire_type", "")
        data = value.get("data")

        if wire_type == "length_delimited" and isinstance(data, dict):
            lines.append(f"{indent_str}Field {key} (nested):")
            lines.append(format_packet(data, indent + 1, show_wire_type))
        elif isinstance(data, dict) and all(k.isdigit() for k in data.keys()):
            lines.append(f"{indent_str}Field {key} (repeated):")
            items = []
            for subkey in sorted(data.keys(), key=lambda x: int(x)):
                sub = data[subkey]
                if "data" in sub:
                    items.append(str(sub["data"]))
                else:
                    items.append(str(sub))
            lines.append(f"{indent_str}  [ {', '.join(items)} ]")
        else:
            if isinstance(data, str):
                data_repr = f'"{data}"'
            else:
                data_repr = str(data)
            if show_wire_type:
                lines.append(f"{indent_str}Field {key}: {data_repr} ({wire_type})")
            else:
                lines.append(f"{indent_str}Field {key}: {data_repr}")

    return "\n".join(lines)


async def TcPOnLine(ip, port, key, iv, AutHToKen, reconnect_delay=0.5):
    global online_writer, whisper_writer, region
    while True:
        try:
            reader, writer = await asyncio.open_connection(ip, int(port))
            online_writer = writer
            token_bytes = bytes.fromhex(AutHToKen) if isinstance(AutHToKen, str) else AutHToKen
            writer.write(token_bytes)
            await writer.drain()
            while True:
                data = await reader.read(9999)
                if not data:
                    break
                hex_data = data.hex()
                packet = await DeCode_PackEt(hex_data[10:])
                packet_json = json.loads(packet)
                print(packet_json)
                print(format_packet(packet_json))

                if hex_data.startswith("0500"):
                    try:
                        packet_json = json.loads(await DeCode_PackEt(hex_data[10:]))

                        if ('5' in packet_json and 'data' in packet_json['5'] and
                                '1' in packet_json['5']['data'] and '8' in packet_json['5']['data']):
                            owner = packet_json['5']['data']['1']['data']
                            code = packet_json['5']['data']['8']['data']
                            print(f"Invite from {owner}, code {code} - accepting")
                            join_pkt = await MahirAccepted(owner, code, key, iv, region)
                            if join_pkt and online_writer:
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_pkt)
                                print("Join packet sent")
                    except Exception:
                        pass
                if hex_data.startswith("0500"):
                    try:
                        packet_json = json.loads(await DeCode_PackEt(hex_data[10:]))
                        uid, chat_code, squad_code = await GeTSQDaTa(packet_json)

                        if uid is not None:
                            print(f"UID={uid} | ChatCode={chat_code} | SquadCode={squad_code}")
                            INV_JOIN_SQ_CHAT = await AutH_Chat(3, uid, chat_code, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', INV_JOIN_SQ_CHAT)
                        else:
                            pass
                    except Exception as e:
                        print(f"[PARSE ERROR] {e}")
                if hex_data.startswith("0500"):
                    try:
                        if packet_json.get('4', {}).get('data') == 8:
                            field5_data = packet_json.get('5', {}).get('data', {})
                            if field5_data.get('2', {}).get('data') == 5:
                                print(" KICK DETECTED")
                                chat_code = field5_data.get('17', {}).get('data')
                                if chat_code is not None:
                                    try:
                                        LeAvE_ChAnNeL = await leave_channel_req(chat_code, 3, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', LeAvE_ChAnNeL)
                                    except Exception as e:
                                        print(f"Failed to leave channel: {e}")
                                else:
                                    print("Chat code not found in kick packet")
                    except Exception as e:
                        print(f"Failed to process kick: {e}")
                if hex_data.startswith("0500"):
                    try:
                        decoded = await DeCode_PackEt(hex_data[10:])
                        if decoded:
                            packet_json = json.loads(decoded)
                            f2 = packet_json.get("2", {}).get("data")
                            f4 = packet_json.get("4", {}).get("data")

                            if f2 == 5 and f4 == 34:
                                field5_data = packet_json.get("5", {}).get("data", {})

                                joiner_uid = field5_data.get("1", {}).get("data")
                                invitee_type = field5_data.get("10", {}).get("data")

                                squad_id = packet_json.get("1", {}).get("data")
                                if not squad_id:
                                    nested_squad = field5_data.get("2", {}).get("data", {})
                                    squad_id = nested_squad.get("1", {}).get("data")

                                if joiner_uid and squad_id and invitee_type is not None:
                                    try:
                                        joiner_uid = int(joiner_uid)
                                        squad_id = int(squad_id)
                                        invitee_type = int(invitee_type)

                                        print(f"Join request from {joiner_uid} to squad {squad_id}")

                                        accept_pkt = await accept_squad_invite(
                                            squad_id,
                                            joiner_uid,
                                            invitee_type,
                                            key,
                                            iv,
                                            region
                                        )
                                        if accept_pkt and online_writer:
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', accept_pkt)
                                            print(" Accept packet sent")
                                    except Exception as e:
                                        print(f" Failed to process join request: {e}")
                                else:
                                    print(" Missing required fields for join request")
                    except Exception as e:
                        print(f"Packet parsing error: {e}")
            writer.close()
            await writer.wait_closed()
            online_writer = None
        except Exception as e:
            print(f"Online error: {ip}:{port} - {e}")
            online_writer = None

        await asyncio.sleep(reconnect_delay)


async def TcPChaT(ip, port, AutHToKen, key, iv, LoGinDaTaUncRypTinG, ready_event, region, reconnect_delay=0.5):
    global spam_room, whisper_writer, spammer_uid, spam_chat_id, spam_uid, online_writer, chat_id, XX, uid, Spy, data2, Chat_Leave, evo_cycle_running, evo_cycle_task, spam_targets, bot_uid
    global pending_ghost_future, ghost_team_id, ghost_squad_code
    global spm_tasks, ToKen, ban_task, ban_running, ban_targets

    print(region, 'TCP CHAT')
    while True:
        try:
            reader, writer = await asyncio.open_connection(ip, int(port))
            whisper_writer = writer
            bytes_payload = bytes.fromhex(AutHToKen) if isinstance(AutHToKen, str) else AutHToKen
            whisper_writer.write(bytes_payload)
            await whisper_writer.drain()
            ready_event.set()
            if LoGinDaTaUncRypTinG.Clan_ID:
                clan_id = LoGinDaTaUncRypTinG.Clan_ID
                clan_compiled_data = LoGinDaTaUncRypTinG.Clan_Compiled_Data
                print('\n - TarGeT BoT in CLan ! ')
                print(f' - Clan Uid > {clan_id}')
                print(f' - BoT ConnEcTed WiTh CLan ChaT SuccEssFuLy ! ')
                pK = await AuthClan(clan_id, clan_compiled_data, key, iv)
                if whisper_writer: whisper_writer.write(pK); await whisper_writer.drain()

            for uid in spam_targets:
                if uid not in spm_tasks or spm_tasks[uid].done():
                    task = asyncio.create_task(spm_loop(uid, key, iv))
                    spm_tasks[uid] = task
                    print(f"Resumed spam for {uid}")

            while True:
                data = await reader.read(9999)
                if not data: break
                if data.hex().startswith("120000"):
                    msg = await DeCode_PackEt(data.hex()[10:])
                    chatdata = json.loads(msg)
                    try:
                        response = await DecodeWhisperMessage(data.hex()[10:])
                        uid = response.Data.uid
                        chat_id = response.Data.Chat_ID
                        XX = response.Data.chat_type
                        inPuTMsG = response.Data.msg.lower()
                    except:
                        response = None
                    if response:

                        # =============== SUB ACCOUNTS COMMANDS ===============
                        try:
                            handled = await handle_sub_command(
                                inPuTMsG, uid, chat_id, response.Data.chat_type, key, iv
                            )
                            if handled:
                                response = None
                                continue
                        except Exception as e:
                            print(f"[SUB-HANDLER] {e}")
                        # ===================================================

                        if inPuTMsG.strip() == '.ready':
                            fields = {
                                1: 15,
                                2: {
                                    1: int(bot_uid),
                                    2: 1
                                }
                            }
                            if region.lower() == "ind":
                                pkt_type = '0514'
                            elif region.lower() == "bd":
                                pkt_type = '0519'
                            else:
                                pkt_type = '0515'

                            packet = await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), pkt_type, key, iv)
                            if packet and online_writer:
                                online_writer.write(packet)
                                await online_writer.drain()
                                await safe_send_message(response.Data.chat_type,
                                                        "[B][C][00FF00] Bot is Ready",
                                                        uid, chat_id, key, iv)
                            else:
                                await safe_send_message(response.Data.chat_type,
                                                        "[B][C][FF0000] Failed to send unready packet.",
                                                        uid, chat_id, key, iv)
                        if inPuTMsG.strip() == '.unready':
                            fields = {
                                1: 15,
                                2: {
                                    1: int(bot_uid)
                                }
                            }
                            if region.lower() == "ind":
                                pkt_type = '0514'
                            elif region.lower() == "bd":
                                pkt_type = '0519'
                            else:
                                pkt_type = '0515'

                            packet = await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), pkt_type, key, iv)
                            if packet and online_writer:
                                online_writer.write(packet)
                                await online_writer.drain()
                                await safe_send_message(response.Data.chat_type,
                                                        "[B][C][00FF00] Bot is not Ready",
                                                        uid, chat_id, key, iv)
                            else:
                                await safe_send_message(response.Data.chat_type,
                                                        "[B][C][FF0000] Failed to send unready packet.",
                                                        uid, chat_id, key, iv)
                        if inPuTMsG.startswith("/6"):
                            try:
                                uid = response.Data.uid
                                chat_id = response.Data.Chat_ID
                                chat_type = response.Data.chat_type
                                message = f"[B][C]{get_random_color()}\n\nPleaSe AccepT My InViTe In 3 SeConDs!!\n\n"
                                P = await SEndMsG(chat_type, message, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                leave_pkt = await ExiT(uid, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave_pkt)
                                await asyncio.sleep(0.5)
                                PAc = await OpEnSq(key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                                C = await cHSq(6, uid, key, iv, region)
                                await asyncio.sleep(0.5)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                                V = await SEnd_InV(6, uid, key, iv, region)
                                await asyncio.sleep(0.5)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                                await asyncio.sleep(3.5)
                                E = await ExiT(None, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)
                            except Exception as e:
                                print(f"Creating team 6: {e}")

                        if inPuTMsG.startswith("/5"):
                            try:
                                uid = response.Data.uid
                                chat_id = response.Data.Chat_ID
                                chat_type = response.Data.chat_type
                                message = f"[B][C]{get_random_color()}\n\nPleaSe AccepT My InViTe In 3 SeConDs!!\n\n"
                                P = await SEndMsG(chat_type, message, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                leave_pkt = await ExiT(uid, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave_pkt)
                                await asyncio.sleep(0.5)
                                PAc = await OpEnSq(key, iv, region)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', PAc)
                                C = await cHSq(5, uid, key, iv, region)
                                await asyncio.sleep(0.5)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', C)
                                V = await SEnd_InV(5, uid, key, iv, region)
                                await asyncio.sleep(0.5)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                                await asyncio.sleep(3.5)
                                E = await ExiT(None, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', E)
                            except Exception as e:
                                print(f"Creating team 5: {e}")

                        if inPuTMsG.strip().startswith('/evos'):
                            try:
                                dd = chatdata['5']['data']['16']
                                message = f"[B][C]{get_random_color()}\n\nLệnh này chỉ hoạt động trong đội! \n\n"
                                P = await SEndMsG(response.Data.chat_type, message, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            except:
                                print('Processing evo cycle start command for evos in team')
                                parts = inPuTMsG.strip().split()
                                uids = [str(response.Data.uid)]
                                if len(parts) > 1:
                                    for part in parts[1:]:
                                        if part.isdigit() and len(part) >= 7:
                                            uids.append(part)
                                if evo_cycle_task and not evo_cycle_task.done():
                                    evo_cycle_running = False
                                    evo_cycle_task.cancel()
                                    await asyncio.sleep(0.5)
                                evo_cycle_running = True
                                evo_cycle_task = asyncio.create_task(evo_gun_cycle(uids, EVO_IDS, key, iv, region))
                                success_msg = f"[B][C]{get_random_color()}\n\n Đã kích hoạt Evo Cycle\n\n"
                                P = await SEndMsG(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                        if inPuTMsG.strip() == '/sevos':
                            if evo_cycle_task and not evo_cycle_task.done():
                                evo_cycle_running = False
                                evo_cycle_task.cancel()
                                success_msg = f"[B][C]{get_random_color()}\n\n Đã dừng Evo Cycle thành công\n\n"
                                P = await SEndMsG(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                print("Evolution emote cycle stopped by command")
                            else:
                                error_msg = f"[B][C]{get_random_color()}\n\n Không có Evo Cycle nào đang chạy\n\n"
                                P = await SEndMsG(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                        if inPuTMsG.startswith('/join '):
                            CodE = inPuTMsG.split('/join ')[1].strip()
                            try:
                                EM = await JoinSq_original(CodE, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', EM)
                                success_msg = f"[B][C]{get_random_color()}\n\nJoined The Squad\n\n"
                                P = await SEndMsG(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            except Exception as e:
                                print(f"Join error: {e}")
                                error_msg = f"[B][C]{get_random_color()}\n\nFailed to join squad\n\n"
                                P = await SEndMsG(response.Data.chat_type, error_msg, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                        if inPuTMsG.startswith('/gh '):
                            parts = inPuTMsG.split(' ', 2)
                            if len(parts) >= 3:
                                teamcode = parts[1].strip()
                                name = parts[2].strip()
                                if bot_uid is not None:
                                    try:
                                        pending_ghost_future = asyncio.Future()
                                        join_pkt = await JoinSq_original(teamcode, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_pkt)
                                        try:
                                            await asyncio.wait_for(pending_ghost_future, timeout=8.0)
                                        except asyncio.TimeoutError:
                                            reply = f"[B][C]{get_random_color()} No response from squad"
                                            P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                            pending_ghost_future = None
                                            continue
                                        leave_pkt = await ExiT(None, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave_pkt)
                                        await asyncio.sleep(0.2)
                                        for _ in range(4):
                                            ghost_pkt = await GhostPakcet(ghost_team_id, name, ghost_squad_code, key, iv)
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', ghost_pkt)
                                            await asyncio.sleep(0.1)
                                        reply = f"[B][C]{get_random_color()} Joined squad {teamcode} as {name}"
                                        P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                        pending_ghost_future = None
                                    except Exception as e:
                                        reply = f"[B][C]{get_random_color()} Failed: {str(e)}"
                                        P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                        pending_ghost_future = None
                                else:
                                    reply = f"[B][C]{get_random_color()} Bot UID not available"
                                    P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            else:
                                reply = f"[B][C]{get_random_color()} Usage: /ghost <squad_code> <name>"
                                P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                        if inPuTMsG.startswith('/lag '):
                            parts = inPuTMsG.strip().split()
                            if len(parts) >= 2:
                                teamcode = parts[1].strip()
                                if bot_uid is not None:
                                    try:
                                        start_msg = f"[B][C]{get_random_color()} Starting Lag attack on squad {teamcode} (30 times)"
                                        P = await SEndMsG(response.Data.chat_type, start_msg, uid, chat_id, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                                        for i in range(30):
                                            try:
                                                join_pkt = await JoinSq_original(teamcode, key, iv)
                                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_pkt)
                                                await asyncio.sleep(0.3)

                                                lag_pkt = await xSKINZxLag(key, iv)
                                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', lag_pkt)
                                                await asyncio.sleep(0.2)

                                                leave_pkt = await ExiT(None, key, iv)
                                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave_pkt)

                                                if (i + 1) % 5 == 0 or i == 29:
                                                    progress_msg = f"[B][C]{get_random_color()} Lag attack progress: {i+1}/30"
                                                    P = await SEndMsG(response.Data.chat_type, progress_msg, uid, chat_id, key, iv)
                                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                                                await asyncio.sleep(0.2)

                                            except Exception as e:
                                                print(f"Lag loop error at {i}: {e}")
                                                continue

                                        done_msg = f"[B][C]{get_random_color()} Lag attack completed on squad {teamcode}"
                                        P = await SEndMsG(response.Data.chat_type, done_msg, uid, chat_id, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                                    except Exception as e:
                                        reply = f"[B][C]{get_random_color()} Failed: {str(e)}"
                                        P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                else:
                                    reply = f"[B][C]{get_random_color()} Bot UID not available"
                                    P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            else:
                                reply = f"[B][C]{get_random_color()} Usage: /lag <squad_code>"
                                P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            continue

                        if inPuTMsG.startswith('/msg '):
                            try:
                                parts = inPuTMsG.strip().split(None, 2)
                                if len(parts) >= 3:
                                    t_code = parts[1]
                                    t_msg = parts[2]
                                    await safe_send_message(XX, f"[00FF00]DoNe SenD MessaGeS In SqWaD", uid, chat_id, key, iv)

                                    async def run_msg_attack():
                                        try:
                                            join_p = await GenJoinSquadsPacket(t_code, key, iv)
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', join_p)
                                            await asyncio.sleep(1.5)
                                            for _ in range(30):
                                                t_msg_colored = f"[b][c]{get_random_color()}{t_msg}"
                                                msg_p = await SEndMsG(XX, t_msg_colored, uid, chat_id, key, iv)
                                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', msg_p)
                                                await asyncio.sleep(0.3)
                                            exit_p = await ExiT(None, key, iv)
                                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', exit_p)
                                        except Exception as e:
                                            print(f"MsG aTTaCk ERRoR: {e}")
                                            await safe_send_message(XX, f"[FF0000]FailDe {str(e)}", uid, chat_id, key, iv)

                                    asyncio.create_task(run_msg_attack())
                                else:
                                    await safe_send_message(XX, "[FF0000]Usage: /msg [teamcode] [message]", uid, chat_id, key, iv)
                            except Exception as e:
                                print(f"MSG Command Error: {e}")
                            continue

                        if inPuTMsG.startswith('/spm '):
                            parts = inPuTMsG.strip().split()
                            if len(parts) >= 2:
                                target_uid = parts[1].strip()
                                if target_uid.isdigit():
                                    if target_uid in spm_tasks:
                                        spm_tasks[target_uid].cancel()
                                        del spm_tasks[target_uid]
                                    spam_targets.add(target_uid)
                                    save_spam_targets()
                                    task = asyncio.create_task(spm_loop(target_uid, key, iv))
                                    spm_tasks[target_uid] = task
                                    reply = f"[B][C]{get_random_color()} DoNe Spam reqests Sqwad"
                                else:
                                    reply = f"[B][C]{get_random_color()} Bro Lezem T7ot uid"
                            else:
                                reply = f"[B][C]{get_random_color()} Usage: /spm <uid>"
                            P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            continue

                        if inPuTMsG.startswith('/stp '):
                            parts = inPuTMsG.strip().split()
                            if len(parts) >= 2:
                                target_uid = parts[1].strip()
                                if target_uid in spm_tasks:
                                    spm_tasks[target_uid].cancel()
                                    del spm_tasks[target_uid]
                                    if target_uid in spam_targets:
                                        spam_targets.remove(target_uid)
                                        save_spam_targets()
                                    reply = f"[B][C]{get_random_color()} DoNe Stopped spam Ya Broo "
                                else:
                                    reply = f"[B][C]{get_random_color()} No active spam"
                            else:
                                reply = f"[B][C]{get_random_color()} Usage: /stp <uid>"
                            P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            continue

                        if inPuTMsG.startswith('/exit'):
                            leave = await ExiT(uid, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'OnLine', leave)
                            success_msg = f"[B][C]{get_random_color()}\n\nLeave The Squad\n\n"
                            P = await SEndMsG(response.Data.chat_type, success_msg, uid, chat_id, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                        if inPuTMsG.strip() == "/admin":
                            uid = response.Data.uid
                            chat_id = response.Data.Chat_ID
                            msg = f"""
[b][c]{get_random_color()} جميع أنواع البوتات متوفرة

[b][c]{get_random_color()} بوتات مقابر
[b][c]{get_random_color()} بوتات غلوري
[b][c]{get_random_color()} بوتات صديق
[b][c]{get_random_color()} بوتات كلان...

[b][c]{get_random_color()} للشراء تواصل معنا:

[b][c][FFFFFF]إنستغرام: @ziwr
[b][c][FFFFFF]تيليجرام: @abdouuuvip

[b][c]{get_random_color()} شكرًا لاستخدام {name} BoT
"""
                            P = await SEndMsG(response.Data.chat_type, msg, uid, chat_id, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                        if inPuTMsG.startswith('/cs5'):
                            try:
                                pkt1 = await create_lw5_packet1(key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', pkt1)
                                await asyncio.sleep(0.2)
                                pkt2 = await create_lw5_packet2(key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', pkt2)
                                reply = f"[B][C]{get_random_color()} lw5 packets sent"
                                P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                V = await SEnd_InV(5, uid, key, iv, region)
                                await asyncio.sleep(0.5)
                                await SEndPacKeT(whisper_writer, online_writer, 'OnLine', V)
                            except Exception as e:
                                reply = f"[B][C]{get_random_color()} Error: {str(e)}"
                                P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                        if inPuTMsG.strip() == "/help":
                            uid = response.Data.uid
                            chat_id = response.Data.Chat_ID
                            msg1 = f"""
[b][c]{get_random_color()} Create a squad

[b][c][FFFFFF] --> /5 , /6

[b][c]{get_random_color()} - Spam join to squad

[b][c][FFFFFF] --> /spm <uid>

[b][c]{get_random_color()} - Stop join spam

[b][c][FFFFFF] --> /stp <uid>

[b][c]{get_random_color()} - Bot join squad

[b][c][FFFFFF] --> /join <teamcode>

[b][c]{get_random_color()} - Send ghosts to squad

[b][c][FFFFFF] --> /gh <teamcode> <name>
"""
                            msg2 = f"""
[b][c]{get_random_color()} - Send messages to squad

[b][c][FFFFFF] --> /msg <teamcode> <name>

[b][c]{get_random_color()} - Player info

[b][c][FFFFFF] --> /info <uid>

[b][c]{get_random_color()} - Check player ban

[b][c][FFFFFF] --> /check <uid>

[b][c]{get_random_color()} - Developer info

[b][c][FFFFFF] --> /admin
"""
                            msg3 = f"""
[b][c]{get_random_color()} ═══ Sub Accounts ═══

[b][c][FFFFFF] /sub add <uid> <pass> [name]
[b][c][FFFFFF] /sub remove <uid>
[b][c][FFFFFF] /sub list
[b][c][FFFFFF] /sub start <uid>
[b][c][FFFFFF] /sub stop <uid>
[b][c][FFFFFF] /sub startall
[b][c][FFFFFF] /sub stopall

[b][c]{get_random_color()} ═══ Multi Squad Spam ═══

[b][c][FFFFFF] /squadspam <teamcode> [count] [delay]
[b][c][FFFFFF] /squadstop
"""
                            P = await SEndMsG(response.Data.chat_type, msg1, uid, chat_id, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            await asyncio.sleep(0.3)
                            P = await SEndMsG(response.Data.chat_type, msg2, uid, chat_id, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            await asyncio.sleep(0.3)
                            P = await SEndMsG(response.Data.chat_type, msg3, uid, chat_id, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                        if inPuTMsG.startswith("/add ") or inPuTMsG.startswith("/remove "):
                            if str(uid) == admin_uid:
                                if inPuTMsG.startswith("/add "):
                                    parts = inPuTMsG.strip().split()
                                    if len(parts) >= 2:
                                        target_uid = parts[1].strip()
                                        if target_uid.isdigit():
                                            try:
                                                success, message = await send_friend_request(target_uid, ToKen, region)
                                                reply = f"[B][C]{get_random_color()} {message}"
                                            except Exception as e:
                                                reply = f"[B][C]{get_random_color()} Error: {str(e)}"
                                        else:
                                            reply = f"[B][C]{get_random_color()} Invalid UID"
                                    else:
                                        reply = f"[B][C]{get_random_color()} Usage: /add <uid>"

                                    P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                    continue

                                if inPuTMsG.startswith("/remove "):
                                    parts = inPuTMsG.strip().split()
                                    if len(parts) >= 2:
                                        target_uid = parts[1].strip()
                                        if target_uid.isdigit():
                                            try:
                                                success, message = await remove_friend(target_uid, ToKen, region)
                                                reply = f"[B][C]{get_random_color()} {message}"
                                            except Exception as e:
                                                reply = f"[B][C]{get_random_color()} Error: {str(e)}"
                                        else:
                                            reply = f"[B][C]{get_random_color()} Invalid UID"
                                    else:
                                        reply = f"[B][C]{get_random_color()} Usage: /remove <uid>"

                                    P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                    continue
                            else:
                                reply = f"[B][C]{get_random_color()} This command is for developer only"
                                P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                continue

                        if inPuTMsG.startswith("/check "):
                            parts = inPuTMsG.strip().split()
                            if len(parts) >= 2:
                                target_uid = parts[1].strip()
                                if target_uid.isdigit():
                                    try:
                                        success, result = await check_banned(target_uid)
                                        if success:
                                            reply = format_check_result(result)
                                        else:
                                            reply = f"[B][C]{get_random_color()} ERROR: {result}"
                                    except Exception as e:
                                        reply = f"[B][C]{get_random_color()} ERROR: {str(e)}"
                                else:
                                    reply = f"[B][C]{get_random_color()} Invalid UID"
                            else:
                                reply = f"[B][C]{get_random_color()} Usage: /check <uid>"

                            P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            continue

                        if inPuTMsG.startswith("/info "):
                            parts = inPuTMsG.strip().split()
                            if len(parts) >= 2:
                                target_uid = parts[1].strip()
                                if target_uid.isdigit():
                                    try:
                                        wait_msg = f"[B][C]{get_random_color()} Fetching player info for {target_uid}..."
                                        P = await SEndMsG(response.Data.chat_type, wait_msg, uid, chat_id, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)

                                        success, data = await get_player_info(target_uid)
                                        if success:
                                            msg1, msg2, msg3 = format_player_info(data)

                                            P1 = await SEndMsG(response.Data.chat_type, msg1, uid, chat_id, key, iv)
                                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P1)
                                            await asyncio.sleep(0.5)

                                            P2 = await SEndMsG(response.Data.chat_type, msg2, uid, chat_id, key, iv)
                                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P2)
                                            await asyncio.sleep(0.5)

                                            P3 = await SEndMsG(response.Data.chat_type, msg3, uid, chat_id, key, iv)
                                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P3)
                                        else:
                                            reply = f"[B][C]{get_random_color()} ERROR: {data}"
                                            P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                            await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                    except Exception as e:
                                        reply = f"[B][C]{get_random_color()} ERROR: {str(e)}"
                                        P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                        await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                else:
                                    reply = f"[B][C]{get_random_color()} Invalid UID"
                                    P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            else:
                                reply = f"[B][C]{get_random_color()} Usage: /info <uid>"
                                P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            continue

                        if inPuTMsG.startswith("/ban "):
                            parts = inPuTMsG.strip().split()
                            if len(parts) >= 2:
                                target_uid = parts[1].strip()
                                if target_uid.isdigit():
                                    if ban_task is not None and not ban_task.done():
                                        ban_running = False
                                        ban_task.cancel()
                                        ban_task = None
                                        ban_targets.clear()
                                        await asyncio.sleep(0.5)

                                    ban_targets.add(target_uid)
                                    ban_task = asyncio.create_task(ban_loop(key, iv))

                                    reply = f"[B][C]{get_random_color()} Ban loop started"
                                    P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                                else:
                                    reply = f"[B][C]{get_random_color()} Invalid UID"
                                    P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                    await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            else:
                                reply = f"[B][C]{get_random_color()} Usage: /ban <uid>"
                                P = await SEndMsG(response.Data.chat_type, reply, uid, chat_id, key, iv)
                                await SEndPacKeT(whisper_writer, online_writer, 'ChaT', P)
                            continue

                        response = None
            whisper_writer.close(); await whisper_writer.wait_closed(); whisper_writer = None
        except Exception as e:
            print(f"Lỗi - {ip}:{port} - {e}"); whisper_writer = None
        await asyncio.sleep(reconnect_delay)


async def MaiiiinE():
    global bot_uid, ToKen, region
    Uid, Pw = '7978478861', 'B88379415CCF5546D950ED6F085C9899C3AEAAAD13E56B3C42C649EFAFA83FE1'
    open_id, access_token = await GeNeRaTeAccEss(Uid, Pw)
    if not open_id or not access_token: print("Lỗi - Tài khoản không hợp lệ"); return None
    platform_id = 2
    PyL = await EncRypTMajoRLoGin(open_id, access_token, platform_id)
    MajoRLoGinResPonsE = await MajorLogin(PyL)
    if not MajoRLoGinResPonsE:
        try:
            platform_id = await InspectToken(access_token)
        except:
            platform_id = 2
        PyL = await EncRypTMajoRLoGin(open_id, access_token, platform_id)
        MajoRLoGinResPonsE = await MajorLogin(PyL)
        if not MajoRLoGinResPonsE: print("Lỗi - Tài khoản đã bị ban hoặc chưa được tạo ! "); return None
    MajoRLoGinauTh = await DecRypTMajoRLoGin(MajoRLoGinResPonsE)
    UrL = MajoRLoGinauTh.url
    print(UrL)
    region = MajoRLoGinauTh.region
    ToKen = MajoRLoGinauTh.token
    TarGeT = MajoRLoGinauTh.account_uid
    bot_uid = TarGeT
    key = MajoRLoGinauTh.key
    iv = MajoRLoGinauTh.iv
    timestamp = MajoRLoGinauTh.timestamp
    LoGinDaTa = await GetLoginData(UrL, PyL, ToKen)
    if not LoGinDaTa: print("Lỗi - Không lấy được cổng (Port) từ dữ liệu đăng nhập!"); return None
    LoGinDaTaUncRypTinG = await DecRypTLoGinDaTa(LoGinDaTa)
    OnLinePorTs = LoGinDaTaUncRypTinG.Online_IP_Port
    ChaTPorTs = LoGinDaTaUncRypTinG.AccountIP_Port
    if ':' not in OnLinePorTs:
        OnLinePorTs = "0.0.0.0:0"
    if ':' not in ChaTPorTs:
        ChaTPorTs = "0.0.0.0:0"
    OnLineiP, OnLineporT = OnLinePorTs.split(":")
    ChaTiP, ChaTporT = ChaTPorTs.split(":")
    acc_name = LoGinDaTaUncRypTinG.AccountName
    print(ToKen)
    equie_emote(ToKen, UrL)

    ChatAutH = create_auth_token_chat(int(TarGeT), ToKen, int(timestamp), key, iv)
    OnlineAutH_packet, enc_token = build_ob55_online_auth(int(TarGeT), ToKen, int(timestamp), key, iv)

    ready_event = asyncio.Event()
    task1 = asyncio.create_task(TcPChaT(ChaTiP, ChaTporT, ChatAutH, key, iv, LoGinDaTaUncRypTinG, ready_event, region))
    await ready_event.wait()
    await asyncio.sleep(1)
    task2 = asyncio.create_task(TcPOnLine(OnLineiP, OnLineporT, key, iv, OnlineAutH_packet))

    # تشغيل الحسابات الفرعية تلقائياً
    asyncio.create_task(auto_start_all_subs())

    print(render('TCP', colors=['white', 'green'], align='center'))
    print('')
    print(f" - Khu vực => {region}".format(region))
    print(f" - Bot đang online, id bot: {TarGeT} | Tên bot : {acc_name}\n")
    print(f" - Bot by | PLong TCP ! (:")
    await asyncio.gather(task1, task2)


async def StarTinG():
    while True:
        try:
            await asyncio.wait_for(MaiiiinE(), timeout=7 * 60 * 60)
        except asyncio.TimeoutError:
            print("Token hết hạn !, Đang khởi động lại ...")
        except Exception as e:
            print(f"Lỗi - {e} => Đang khởi động lại ...")