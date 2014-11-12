function IdPool(){
    this.pool = [];
}

function IdNum(id){
    this.avail = true;
    this.id = id;
}

IdNum.prototype.block = function(){
    this.avail = false;
};

IdNum.prototype.valueOf = function(){
    return this.id;
};

IdNum.prototype.value = function(){
    return this.id;
};

IdNum.prototype.release = function(){
    this.avail = true;
};
IdPool.prototype.get = function(){
    if (this.pool.filter(function(x){ return x.avail;}).length === 0)
        this.pool.push(new IdNum(this.pool.length));
    var id = this.pool.filter(function(x){ return x.avail; })[0];
    id.block();
    return id;
};

module.exports = IdPool;
