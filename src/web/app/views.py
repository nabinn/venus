from app import app
from flask import Flask, jsonify, render_template, request, redirect, url_for
from flask import jsonify
import mysql.connector

db_config={
	'host':'<host_IP>',
	'user':'<user_name>',
	'password':'<password>',
	'database':'<database_name>'
}

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")


@app.route('/result', methods = ['POST'])
def result():
    date = request.form["day"]
    analytics= request.form["analytics"]
    top_n = int(request.form["num"])

    if analytics=="active_pairs":
        query2 = """
        SELECT user1, user2, transaction_freq
        FROM pair_activity
        WHERE transaction_date=%s
        ORDER BY transaction_freq DESC LIMIT %s
        """

        query ="""
		SELECT CONCAT(u1.firstname, " ", u1.lastname) AS user1,
		u1.picture AS user1_img,
		CONCAT(u2.firstname, " ", u2.lastname) AS user2,
		u2.picture AS user2_img,
		p.transaction_freq FROM  pair_activity p
		INNER JOIN users u1 on u1.id=p.user1
		INNER JOIN users u2 on u2.id=p.user2
		WHERE p.transaction_date=%s
		ORDER BY p.transaction_freq DESC LIMIT %s
		"""

        cursor.execute(query, (date,top_n))
        result=[]
        for pair in cursor.fetchall():
            result.append(pair)
        resp = [{"user1": x[0],"user1_img":x[1], "user2": x[2],"user2_img":x[3], "transaction_freq": x[4] } for x in result]
        return render_template("active_pairs.html", data=resp, title="Top "+str(top_n)+
								" most active pair of users on "+date)

    elif analytics=="active_senders":
        query = """
        SELECT u.firstname, u.lastname, u.picture, s.send_freq
        FROM users u INNER JOIN sender_activity s ON
         s.user_id=u.id
         WHERE transaction_date=%s
         ORDER BY s.send_freq DESC LIMIT %s
        """
        cursor.execute(query, (date,top_n))
        result=[]
        for x in cursor.fetchall():
            result.append(x)
        resp = [{"name": x[0]+" "+x[1],"image":x[2],"send_freq": x[3] } for x in result]
        return render_template("active_senders.html", data=resp, title="Top "+str(top_n)+
		" most active senders on "+date)

    elif analytics=="active_receivers":
        query = """
        SELECT u.firstname, u.lastname, u.picture, r.recv_freq
        FROM users u INNER JOIN receiver_activity r ON
         r.user_id=u.id
         WHERE transaction_date=%s
         ORDER BY r.recv_freq DESC LIMIT %s
        """
        cursor.execute(query, (date,top_n))
        result=[]
        for x in cursor.fetchall():
            result.append(x)
        resp = [{"name": x[0]+" "+x[1],"image":x[2],"recv_freq": x[3] } for x in result]
        return render_template("active_receivers.html", data=resp, title="Top "+str(top_n)+" most active Receivers on "+date)


    elif analytics=="active_spenders":
        query = """
        SELECT u.firstname, u.lastname, u.picture, n.amount
        FROM net_spending n INNER JOIN users u ON
         n.user_id=u.id
         WHERE transaction_date=%s
         ORDER BY n.amount DESC LIMIT %s
        """
        cursor.execute(query, (date,top_n))
        result=[]
        for x in cursor.fetchall():
            result.append(x)
        resp = [{"name": x[0]+" "+x[1],"image":x[2],"amount": x[3]/100 } for x in result]
        return render_template("active_spenders.html", data=resp, title="Top "+str(top_n)+" Spenders on "+date)

    else:
        print("invalid choice")
