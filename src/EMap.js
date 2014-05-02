/*
Jordan D Chapian | http://www.jordanchapian.com
PYA Analytics    | http://www.pyaanalytics.com
*/

//TODO: Set up cleanup routine with an animation frame to handle straggling expired data every 30s
//moving average
//outlier detection for heatmap maximum values

(function(){
var strKey = {second:1, minute:60, hour:3600, day:86400, week:604800}
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
var Record = function(data, ttl, tts){
    //creation information
    this._nfo = {};
    this._nfo.created = new Date().getTime();
    this._nfo.activeUntil = this._nfo.created + ttl;
    this._nfo.cacheUntil = this._nfo.activeUntil + tts;

    //extend the record with the supplied values
    _.extend(this, data);
}
Record.fn = Record.prototype;

//[data](object), [rules]([Aggregate](object))
//apply a new object to a currently active object based on aggregation rules
Record.fn._apply = function(data, rules){
    for(var rName in rules){
        rules[rName].apply(data, this);
    }
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

var Grain = function(ttl, tts){
    //set up global properties
    this.ttl = toS(ttl) * 1000;
    this.tts = toS(tts) * 1000;
    this.active = {};
    this.cache = {};
    this.heap = {};
    this.subscriber = {
        'expire':[],
        'new':[],
    };
}
Grain.fn = Grain.prototype;

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
    if(key != '*') collection[key] = key;
    else collection = this.active;
    //perform sweep
    for(var rkey in collection){
        if(_.isUndefined(this.active[rkey]))continue;

        if(!this.active[rkey]._isActive()){
            this.cache[rkey] = (_.isUndefined(this.cache[rkey])) ? [this.active[rkey]] : [this.active[rkey]].concat(this.cache[rkey]);
            delete this.active[rkey];
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

                active:function(){
                    self.sweepActive(key);
                    var result = (key == '*') ? self.active : self.active[key];
                    return result;
                }
            }
    }
}

//on will allow for functions to be attached to internal processes
//[type of event]{string}, [specifier]{null,string,function},[handle]{function}
Grain.fn.on = function(event, specifier, handle){
    if(_.hasKey(this.subscriber, event)) this.subscriber[event] = new Subscriber(event, specifier, handle);
    else; //report error, event not valid
}

/***********************************/
/************AGGREGATE**************/
/***********************************/
var Aggregate = function(key, action){
    this.key = key;
    this.action = action;
    this.subscriber = {
        'applied':[]
    }
    //report any errors
}
Aggregate.fn = Aggregate.prototype;
Aggregate.fn.apply = function(data, record){
    if(_.isUndefined(record[this.key]) || _.isUndefined(data[this.key]))return;
    //apply the aggregation function
    record[this.key] = this.action(record[this.key], data[this.key]);
    //report to subscribers
        //TODO
}
Aggregate.fn.on = function(event, specifier, handle){
    if(_.hasKey(this.subscriber, event)) 
        this.subscriber[event] = new Subscriber(event, specifier, handle);
    else; //report error, event not valid
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

var Map = window.Map = function(granularity, aggregate) {
    
    //init instance objects
    this.grain = {};
    this.aggregate = {};

    //store the granularity
    for(var ttl in granularity) this.grain[ttl] = new Grain(ttl, granularity[ttl]);
    //store the aggregation settings
    for(var key in aggregate) this.aggregate[key] = new Aggregate(key, aggregate[key]);

};
Map.fn = Map.prototype;
Map.fn.version = '0.0.1';

//put is the only interface into all collections
//the reason for this is to maintain consistency between collection aggregations
Map.fn.put = function(key, data) {

    for(var grainName in this.grain){
        //get the grain object
        var grain = this.grain[grainName];
        //get the current stored active value
        var record = grain.get(key).from.active();
        //if it does not exist in the collection, add it
        if(_.isUndefined(record))grain.active[key] = new Record(data, grain.ttl, grain.tts);
        //or just apply the new object to it with supplied rules for combining data
        else record._apply(data, this.aggregate);
    }

};

})();