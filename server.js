require("dotenv").config();

const dns = require("dns");
dns.setServers(["8.8.8.8", "8.8.4.4"]);

const express=require("express");
const http=require("http");
const {Server}=require("socket.io");

const app=express();
const httpserver=http.createServer(app);
const io=new Server(httpserver);

// ===== NEW (1 line): serve the frontend from the "public" folder =====
app.use(express.static("public"));

//to manage active user count
let count=0;
io.on("connection",(socket)=>{
    count++;
    console.log("Active User:",count);//terminal/backend
    socket.emit("active",count);//frontend/client
});


io.on("connection",(socket)=>{
    //To broadcast a message
    socket.broadcast.emit("join","New User Joined");
    socket.on("msg",(cb)=>{
        console.log(cb);
    });

    //handling disconnect event
    socket.on("disconnect",()=>{
        count--;
        console.log("Active User:",count);//terminal/backend
    });
    //for making announcement
    /*io.emit("announce","Only 5hr class left");*/ 
    
    console.log("Connected Established");
    socket.emit("chat","hello from server side");
    socket.on("msg",(cb)=>{
        console.log(cb);
    });
});



// =====================================================================
// NEW FEATURES (everything above is your original code, unchanged)
// usernames, chat rooms, private messages, image sharing, read receipts,
// typing indicator, online list, join/leave notices, MongoDB storage
// =====================================================================
const {MongoClient}=require("mongodb");
// local MongoDB by default; set MONGO_URL to use MongoDB Atlas or another server
const MONGO_URL=process.env.MONGO_URL||"mongodb://127.0.0.1:27017";
const client=new MongoClient(MONGO_URL);
let messages;   // the "messages" collection, set in start() below

const users={};   // socket.id -> username
const rooms=["General","Study","Random"];

const userList=()=>Object.values(users).map(name=>({name}));
const idOf=(name)=>Object.keys(users).find(id=>users[id]===name);
const cleanText=(t)=>String(t||"").trim().slice(0,500);
const cleanImage=(i)=>(typeof i==="string"&&/^data:image\/(png|jpeg|webp|gif);base64,/.test(i)&&i.length<=600000)?i:null;
// turn a MongoDB document into what the frontend expects
const out=(d)=>({id:d._id.toString(),user:d.sender,to:d.receiver,text:d.text,image:d.image,time:d.time,read:!!d.read});
// stops one failed database call from crashing the whole server
const safe=(fn)=>async(...args)=>{ try{ await fn(...args); }catch(e){ console.error("Error:",e.message); } };

io.on("connection",(socket)=>{

    // 1. Choose a username
    socket.on("setUsername",(name)=>{
        name=String(name||"").trim().slice(0,20);
        if(!name) return socket.emit("usernameError","Enter a name to continue.");
        if(Object.values(users).some(u=>u.toLowerCase()===name.toLowerCase())){
            return socket.emit("usernameError",`"${name}" is already taken. Try another.`);
        }
        users[socket.id]=name;
        socket.emit("joined",{id:socket.id,name,rooms});
        io.emit("userList",userList());
        socket.broadcast.emit("systemMsg",`${name} joined the chat`);
    });

    // 2. Rooms
    socket.on("createRoom",(room)=>{
        room=String(room||"").trim().slice(0,20);
        if(!users[socket.id]||!room) return;
        if(rooms.some(r=>r.toLowerCase()===room.toLowerCase())) return;
        rooms.push(room);
        io.emit("roomList",rooms);
    });
    socket.on("loadRoom",safe(async(room)=>{
        if(!users[socket.id]||!rooms.includes(room)) return;
        const docs=await messages.find({room}).sort({_id:-1}).limit(50).toArray();
        socket.emit("roomHistory",{room,messages:docs.reverse().map(out)});
    }));
    socket.on("roomMsg",safe(async({room,text,image})=>{
        const user=users[socket.id];
        text=cleanText(text); image=cleanImage(image);
        if(!user||!rooms.includes(room)||(!text&&!image)) return;
        const doc={room,sender:user,receiver:null,text,image,time:Date.now(),read:false};
        await messages.insertOne(doc);   // adds doc._id
        io.emit("roomMsg",{...out(doc),room});
    }));

    // 3. Private messages + read receipts
    socket.on("loadDm",safe(async(other)=>{
        const me=users[socket.id];
        if(!me) return;
        const docs=await messages.find({room:null,$or:[{sender:me,receiver:other},{sender:other,receiver:me}]})
            .sort({_id:-1}).limit(50).toArray();
        socket.emit("dmHistory",{with:other,messages:docs.reverse().map(out)});
    }));
    socket.on("dmMsg",safe(async({to,text,image})=>{
        const user=users[socket.id], toId=idOf(to);
        text=cleanText(text); image=cleanImage(image);
        if(!user||!toId||(!text&&!image)) return;
        const doc={room:null,sender:user,receiver:to,text,image,time:Date.now(),read:false};
        await messages.insertOne(doc);
        const m=out(doc);
        io.to(toId).emit("dmMsg",m);
        socket.emit("dmMsg",m);
    }));
    socket.on("markRead",safe(async({with:other})=>{
        const me=users[socket.id], otherId=idOf(other);
        if(!me) return;
        const r=await messages.updateMany({room:null,sender:other,receiver:me,read:false},{$set:{read:true}});
        if(r.modifiedCount>0&&otherId) io.to(otherId).emit("messagesRead",{by:me});
    }));

    // 4. Typing indicator (room or private)
    socket.on("typing",({room,to,isTyping})=>{
        const user=users[socket.id];
        if(!user) return;
        const p={user,isTyping:!!isTyping};
        if(to){ const id=idOf(to); if(id) io.to(id).emit("typing",{...p,private:true}); }
        else socket.broadcast.emit("typing",{...p,room});
    });

    // 5. Leaving
    socket.on("disconnect",()=>{
        const name=users[socket.id];
        if(!name) return;
        delete users[socket.id];
        io.emit("userList",userList());
        io.emit("systemMsg",`${name} left the chat`);
    });
});

// connect to MongoDB first, then start the server
async function start(){
    await client.connect();
    messages=client.db("chatapp").collection("messages");
    await messages.createIndex({room:1,_id:-1});
    await messages.createIndex({sender:1,receiver:1,_id:-1});
    // bring back rooms that were created in earlier runs
    (await messages.distinct("room",{room:{$ne:null}}))
        .forEach(r=>{ if(!rooms.includes(r)) rooms.push(r); });
    httpserver.listen(8080,()=>{
        console.log("Server Started");
    });
}
start().catch(err=>{
    console.error("Could not connect to MongoDB:",err.message);
    process.exit(1);
});