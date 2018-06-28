require "mongo"
require 'elasticsearch'
class Bulk
  def initialize
    @esclient = Elasticsearch::Client.new host:'localhost:9200'
    @client = Mongo::Client.new('mongodb://127.0.0.1:27017/test') #mongo connection with db name
    @collection = @client[:roc] #collection name here
	end
  
  attr_reader :esclient, :collection
  
  def execute
    data = []
    collection.find().each do |record|
      result = {}
      result['name'] = record['name'] #key's here
      temp = {}
      temp["index"]={}
      temp["index"]['_index'] = 'index name here'
      temp["index"]['_type'] = 'type name here'
      #temp["index"]['_id'] = by default elastic generate the index id
      temp["index"]['data'] = result
      puts result
      puts "=" * 60
      data << temp
      if data.length.to_i >= 10
        esclient.bulk body: data
        data = []
        puts data.length.to_i
      end
    end
    if data.length.to_i > 0
      esclient.bulk body: data
      data = []
      puts data.length.to_i
    end
  end
end
obj = Bulk.new
obj.execute
