# =========================
# 🚀 WEBHOOK (UPDATED v2.7.1)
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("📩 PAYLOAD:", data)

    symbol = data.get("symbol")
    decision = data.get("decision_model")
    price = data.get("price")
    momentum = data.get("momentum_strength")
    trend = data.get("trend_strength")
    alignment = data.get("trend_alignment")
    data_version = data.get("data_version")

    conn = get_db()
    cur = conn.cursor()

    now = datetime.utcnow()

    try:
        # TAGGING
        abs_mom = abs(momentum) if momentum else 0
        abs_trend = abs(trend) if trend else 0

        trend_quality = "strong" if abs_trend > 0.25 else "medium" if abs_trend > 0.12 else "weak"
        momentum_state = "impulse" if abs_mom > 0.45 else "build" if abs_mom > 0.2 else "weak"

        score = (
            (2 if abs_mom > 0.45 else 1 if abs_mom > 0.2 else 0) +
            (2 if abs_trend > 0.25 else 1 if abs_trend > 0.12 else 0) +
            (1 if alignment == "aligned" else 0)
        )

        signal_quality = "A+" if score >= 5 else "B" if score >= 3 else "C"

        # EXIT ENGINE (unchanged)
        cur.execute("""
            SELECT id, symbol, direction, entry_price,
                   stop_price, target_price, opened_at
            FROM bot_trades
            WHERE status = 'OPEN'
        """)

        for trade_id, t_symbol, direction, entry, stop, target, opened in cur.fetchall():
            if t_symbol != symbol:
                continue

            duration = (now - opened).total_seconds()
            pnl = ((price - entry) / entry * 100) if direction == "LONG" else ((entry - price) / entry * 100)

            close = False

            if (direction == "LONG" and (price >= target or price <= stop)) or \
               (direction == "SHORT" and (price <= target or price >= stop)):
                close = True

            if not close and duration > 21600 and pnl < 0.2:
                close = True

            if not close and duration > 43200:
                close = True

            if close:
                cur.execute("""
                    UPDATE bot_trades
                    SET status = 'CLOSED',
                        closed_at = NOW(),
                        pnl_percent = %s
                    WHERE id = %s
                """, (pnl, trade_id))

        # =========================
        # ENTRY FILTER
        # =========================
        hold_reason = None

        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"

        elif alignment != "aligned":
            hold_reason = "counter_trend"

        # ✅ TREND FILTER (already added)
        elif abs_trend < 0.12:
            hold_reason = "weak_trend"

        # ✅ NEW: MOMENTUM FILTER
        elif abs_mom < 0.2:
            hold_reason = "weak_momentum"

        # =========================
        # STACKING LOGIC
        # =========================
        if hold_reason is None:
            cur.execute("""
                SELECT entry_price, opened_at
                FROM bot_trades
                WHERE symbol = %s AND status = 'OPEN'
                ORDER BY opened_at ASC
            """, (symbol,))
            existing = cur.fetchall()

            if len(existing) >= 2:
                hold_reason = "trade_exists"
            elif len(existing) == 1:
                first_entry, first_time = existing[0]

                if now - first_time < timedelta(minutes=20):
                    hold_reason = "too_soon"

                elif (decision == "LONG" and price >= first_entry) or \
                     (decision == "SHORT" and price <= first_entry):
                    hold_reason = "no_better_price"

        # SAVE SIGNAL
        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol, decision_model, momentum_strength, trend_strength,
                trend_alignment, price, data_version, hold_reason,
                signal_quality, trend_quality, momentum_state, signal_score, created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            symbol, decision, momentum, trend,
            alignment, price, data_version, hold_reason,
            signal_quality, trend_quality, momentum_state, score
        ))

        # EXECUTE TRADE
        if hold_reason is None:
            stop_price = price * (0.996 if decision == "LONG" else 1.004)
            target_price = price * (1.008 if decision == "LONG" else 0.992)

            cur.execute("""
                INSERT INTO bot_trades (
                    symbol, direction, entry_price,
                    stop_price, target_price,
                    status, data_version, opened_at
                )
                VALUES (%s,%s,%s,%s,%s,'OPEN',%s,NOW())
            """, (
                symbol, decision, price,
                stop_price, target_price, data_version
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("❌ ERROR:", e)

    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "processed"}), 200
