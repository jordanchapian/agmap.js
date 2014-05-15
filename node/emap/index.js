/*
Jordan D Chapian | http://www.jordanchapian.com
PYA Analytics    | http://www.pyaanalytics.com
*/

//TODO: Set up cleanup routine with an animation frame to handle straggling expired data every 30s
//moving average
//outlier detection for heatmap maximum values
//TODO: Write a detector plugin method which will allow the programmer to recieve the data that is changing, and make decisions about it, and mark values on the object

//TODO: Write middleware, beforeware, afterware

var strKey = {ms:1,s:(1*1000), m:(60*1000), h:(3600*1000), d:(86400*1000), w:(604800*1000), infinity:9007199254740992}
var toS = function(str){
    var q = /(\d+)(.*)/.exec(str);
    if(q.length != 3)return 0; //error
    return strKey[q[2]] * Number(q[1]);
}

/***********************************/
/**************RESULT***************/
/***********************************/

var Result = function(data){
    this.data = data;
}
Result.fn = Result.prototype;
Result.fn.between = function(start, end){
//todo
}
Result.fn.where = function(filterOrKey, valOrNull){
    var filter;

    //determine what the filter is
    if(_.isFunction(filterOrKey))
        filter = filterOrKey;
    else if(_.isString(filterOrKey) && _.isString(valOrNull))
        filter = function(e){return e[filterOrKey] == valOrNull}

    //if we can't filter, do nothing
    if(_.isUndefined(filter))return;

    //apply the filter to each granularity
    for(var grain in this.data){
        this.data[grain] = _.filter(this.data[grain], filter);
    }
}
Result.fn.get = function(filterOrKey, valOrNull){
    return this.data;
}

/***********************************/
/**************Record***************/
/***********************************/
var Record = function(data, ttl, tts, key){
    //creation information
    this._nfo = {};
    this._nfo.created = new Date().getTime();
    this._nfo.activeUntil = this._nfo.created + ttl;
    this._nfo.cacheUntil = this._nfo.activeUntil + tts;
    this.key = key;
    //extend the record with the supplied values
    for(var key in data){
        this[key] = data[key];
    }
}
Record.fn = Record.prototype;
Record.fn.timeElapsed = function(){
    return (new Date().getTime() - this._nfo.created);
}
Record.fn._isActive = function(){
    return (this._nfo.activeUntil > new Date().getTime());
}
Record.fn._isCached = function(){
    return (!this._isActive() && (this._nfo.cacheUntil > new Date().getTime()));
}
/***********************************/
/**************GRAIN****************/
/***********************************/

var Grain = function(ttl, tts, map){
    //set up global properties
    this.ttl = toS(ttl);
    this.tts = toS(tts);
    this.active = {};
    this.cache = {};
    this.heap = {};
    this.map = map;

    //init the middleware containers
    this.middleware = {};
    for(var name in this._.middleware){
        this.middleware[this._.middleware[name]] = [];
    }
}
Grain.fn = Grain.prototype;

//settings
Grain.fn._ = {
    middleware:{update:'handleUpdate', new:'handleNew', request:'handleRequest',expire:'handleExpire'}
} 

Grain.fn.arm = function(pluginInstance){

    //extract the exports
    if(pluginInstance.exports){
        for(var exportName in pluginInstance.exports){
            if(!this[exportName]){//ensure no overwrites
                this[exportName] = pluginInstance.exports[exportName];
            }
            else //report overwrite
                console.error('attempted to overwrite granularity with export: '+exportName);
        }
    }

    //extract the middleware specifiers
    for(var name in this._.middleware){
        if(pluginInstance.api[this._.middleware[name]]){
            this.middleware[this._.middleware[name]].push(pluginInstance.api[this._.middleware[name]]);
        }
    }

}

Grain.fn.sweepCache = function(key){
    //determine the set of keys
    var collection = {};
    if(key != '*') collection[key] = key;
    else collection = this.cache;
    //perform sweep
    for(var rkey in collection){
        if(_.isUndefined(this.cache[rkey]))continue;

        var inactive = _.filter(this.cache[rkey], function(e){return !e._isCached();});
        var active = _.filter(this.cache[rkey], function(e){return e._isCached();});

        if(inactive.length > 0){
            this.heap[rkey] = (_.isUndefined(this.heap[rkey])) ? inactive : inactive.concat(this.heap[rkey]);
        }

        if(active.length == 0) delete this.cache[rkey];
        else this.cache[rkey] = active;
    }
}

Grain.fn.sweepActive = function(key){
    //determine the set of keys
    var collection = {};
    if(key && key != '*') collection[key] = key;
    else collection = this.active;
    //perform sweep
    for(var rkey in collection){
        if(!this.active[rkey])continue;

        if(!this.active[rkey]._isActive()){
            //call any existing middleware to handle the event of a expired record
            var middleware = this.middleware[this._.middleware.expire];
            for(var i=0;i<middleware.length;i++){
                middleware[i](this.active[rkey], this);
            }
            //add to cache and remove from active
            this.cache[rkey] = (!this.cache[rkey]) ? [this.active[rkey]] : [this.active[rkey]].concat(this.cache[rkey]);
            delete this.active[rkey];
        }
    }
}

Grain.fn.put = function(key, data){
    //get the current stored active value
    var record = this.get(key).from.active(false);

    //if it does not exist in the collection, add it
    if(!record) {
        record = this.active[key] = new Record(data, this.ttl, this.tts, key);
        //call any existing middleware to handle the event of a new record
        var middleware = this.middleware[this._.middleware.new];
        for(var i=0;i<middleware.length;i++){
            middleware[i](record, this);
        }
    }

    //or just apply the new object to it with supplied rules for combining data
    else 
    {
        var changesMade = false;
        for(var name in this.map.aggregate){
            var result = this.map.aggregate[name].apply(data, record);
            if(changesMade == false)changesMade = result;
        }
        //made a call to the middleware if needed
        if(changesMade){
            //call any existing middleware to handle the event of a new record
            var middleware = this.middleware[this._.middleware.update];
            for(var i=0;i<middleware.length;i++){
                middleware[i](record, this);
            }
        }
    }
}

Grain.fn.get = function(key){
    var self = this;

    return{
        from:{
                cache:function(){
                    self.sweepCache(key);
                    var result = (key == '*') ? self.cache : self.cache[key];
                    return result;
                },

                heap:function(){
                    //worry about heap later, todo
                },

                active:function(report){
                    //clean up inactive keys
                    self.sweepActive(key);
                    var result = (key == '*') ? self.active : self.active[key];
                    if(report == false)return result;
                    //report this to the fetch middlewares
                    var middleware = self.middleware[self._.middleware.request];
                    for(var k in result){
                        for(var i=0;i<middleware.length;i++){
                            middleware[i](result[k], self);
                        }
                    }

                    //return the results
                    return result;
                }
            }
    }
}

/***********************************/
/************AGGREGATE**************/
/***********************************/
var Aggregate = function(key, action){
    this.key = key;
    this.action = action;
    //report any errors
}
Aggregate.fn = Aggregate.prototype;
Aggregate.fn.apply = function(data, record){
    if(!record[this.key] || !data[this.key])return false;
    //apply the aggregation function
    record[this.key] = this.action(record[this.key], data[this.key]);

    return true;
}

/***********************************/
/***********SUBSCRIBER**************/
/***********************************/
var Subscriber = function(event, specifier, action){
    this.event = event;
    this.action = action || specifier;
    this.specifier = (_.isUndefined(action)) ? undefined : specifier;

    //wrap the specifier into a function easily used
}
Subscriber.fn = Subscriber.prototype;
Subscriber.fn.report = function(changes){

}

/***********************************/
/***************MAP*****************/
/***********************************/
//think of the map as an interface into multiple self sustaining granularities
//by providing batch operations

var EMap = function(granularity, aggregate) {
    
    //init instance objects
    this.grain = {};
    this.aggregate = {};

    //store the granularity
    for(var ttl in granularity) this.grain[ttl] = new Grain(ttl, granularity[ttl], this);
    //store the aggregation settings
    for(var key in aggregate) this.aggregate[key] = new Aggregate(key, aggregate[key], this);

};
EMap.fn = EMap.prototype;

//accepts an emap.plugin
EMap.fn.arm = function(){
    //apply all supplied plugins to all granularities
    var arguments = Array.prototype.slice.call(arguments);
    for(var i=0; i < arguments.length; i++){

        var plugin = arguments[i];
        for(var grain in this.grain){
            this.grain[grain].arm(plugin.getInstance(this.grain[grain]));
        }

    }
}

EMap.fn.put = function(key, data) {

    for(var grainName in this.grain){
        this.grain[grainName].put(key,data);
    }

};
     /***********************************/
/************   EMAP.PLUGIN   **************/
     /***********************************/

EMap.plugin = function(name, interface, args){

    this.getInstance = function(grain){
        var r = {
            api: new interface(args),
            exports:undefined
        }

        //record the exported object
        if(r.api.exports){
            r.exports = r.api.exports;
            delete r.api.exports;
        }

        //call the init function if needed
        if(r.api.init){
            r.api.init(grain);
            delete r.api.init;
        }

        return r;
    }
}

module.exports = {
    map : function(granularity, aggregate){
        return new EMap(granularity, aggregate);
    },
    plugin:function(name, interface, args){
        return new EMap.plugin(name, interface, args);
    }
}
