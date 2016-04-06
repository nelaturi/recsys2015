--Short Lua script to calculate input files to perf for VW solution
local function main()
   local predictedFile = assert(io.open('pak','r'))
   local targetsFile = assert(io.open('yoochoose-solution.dat','r'))
   local predicted = {}
   local targets = {}

   for line in predictedFile:lines('*l') do
      local l = line:split(' ')
      predicted[tonumber(l[2])] = tonumber(l[1])
   end
   for line in targetsFile:lines('*l') do
      local l = line:split(';')
      assert(l[1] ~= nil, line)
      targets[tonumber(l[1])] = 1
   end

   local tp = 0
   local fp = 0
   for sessionId,prob in pairs(predicted) do
      if targets[sessionId] and prob > 0.5 then
         tp = tp + 1
      elseif targets[sessionId] == nil and prob > 0.5 then
         fp = fp + 1
      end
   end
   print('TP: ' ..tp.. ', FP: ' ..fp.. ', TP-FP: ' ..tp-fp)
end

main()
