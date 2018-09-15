/* init история изменения */

const api_call = (method, data, wheel, lock_) => {
    lock = new Promise( async (resolve, reject) => {
        let url = 'http://192.168.7.134:8010/api_' + method
        console.debug("API CALL", url, data)

        if (data && typeof data === 'object') {
            url += '?'
            if (data.tagName && data.tagName === 'FORM') {
                url += $(data).serialize()
            } else {
                url += Object.keys(data).map(function(k) {
                    if (k == 'json' && data && typeof data[k] === 'object') {
                        return encodeURIComponent(k) + "=" + encodeURIComponent(JSON.stringify(data[k]));
                    } else if (data[k] != undefined && data[k] != 'undefined') {
                        return encodeURIComponent(k) + "=" + encodeURIComponent(data[k]);
                    }

                }).join('&')
            }
        } else if (typeof data !== 'undefined') {
            console.warn("API", typeof data, data)
            return
        }

        //if (wheel !== false) load()
        $("#wheel").fadeIn();

        try {
            success = await $.getJSON(url)
            
        } catch (error) {
            //if (wheel !== false) unload()
            $("#wheel").fadeOut();
            console.log(error)
            if (error.statusText.search(/([Tt]ime).*([Oo]ut)/) >= 0) {
                console.error("Таймаут при выполнении запроса к", url)
            } else if (error.status == 401) {
                //if (user)
                    //user.logout()
            } else {
                console.error('ERROR', error) //.responseText.replace(/<\/?[^>]+(>|$)/g, "")
            }
            return reject(error) 
        }

        //if (wheel !== false) unload()
        $("#wheel").fadeOut();
        console.debug(success)
        resolve(success)
    })
    //if (lock_) user.lock = lock
    return lock
}

function dump(obj) {
    var out = '';
    for (var i in obj) {
      if (typeof obj[i] === 'object') {
        out += i + ": " + obj[i] + "\n";
        for (var ii in obj[i]) {
          out += "---  " + ii + ": " + obj[i][ii] + "\n";
        }
      } else {
        out += i + ": " + obj[i] + "\n";
      }
    }
    $('div.contextBlock').append('<pre>'+out+'</pre>');
}

//check element isset in array
function contains(a, obj) {
    if (typeof a === "undefined") return false;
    
    for (var i = 0; i < a.length; i++) {
        if (a[i] === obj) {
            return true;
        }
    }
    return false;
}

var dTypeList = [], dViewList = [], dObjList = [], dTypeListND = []

//fill dtypeSel select
function fillDType(dTypeList)
{
  $('select#dtypeSel').empty(); //clear
  $('select#dtypeSel').append($('<option>', { value: "0", text : "-- не выбран --"}));
  $.each(dTypeList, function (i, item) {
    if (typeof item !== "undefined" && item.length > 0) {
      $('select#dtypeSel').append($('<option>', { 
        value: i,
        text : item 
      }));
    }
  });
}

//fill dnameSel select
function fillDName(dViewList)
{
  $('select#dnameSel').empty(); //clear
  $('select#dnameSel').append($('<option>', { value: "0", text : "-- не выбран --"}));
  $.each(dViewList, function (i, item) {
    if (typeof item !== "undefined" && item.length > 0) {
      $('select#dnameSel').append($('<option>', { 
        value: i,
        text : item 
      }));
    }
  });
}

//fill dobjSel select
function fillDObj(dObjList)
{
  $('select#dobjSel').empty(); //clear
  $('select#dobjSel').append($('<option>', { value: "", text : "-- не выбран --"}));
  $.each(dObjList, function (i, item) {
    //if (typeof item !== "undefined" && item.length > 0) {
      $('select#dobjSel').append($('<option>', { 
        value: item.ref_ones,
        text : item.data_string 
      }));
    //}
  });
}

//get matadata
let data
data = api_call('log_get_meta_data', {}, true, true)

//dump(data)

data
  .then(
    result => {
      //dump(result)
      //parse type & view
      for (var i in result) {
        if (typeof result[i]['name'] !== 'undefined') {
          var elName = result[i]['name']
          var elCode = result[i]['code']
          var elName = elName.toString().split(".")
          dTypeList[elCode.toString()] = elName[0]
          dViewList[elCode.toString()] = elName[1]
        }
      }
      
      //del types dubl (Not Dubls)
      //contains(a, obj)
      for (var i in dTypeList) {
        if (!contains(dTypeListND, dTypeList[i])) { dTypeListND[i] = dTypeList[i] }
      }
      
      //dump(dTypeList)
      //dump(dViewList)
      //fill type & view
      fillDType(dTypeListND)
      fillDName(dViewList)
    },
    error => {
      //dump("Rejected: " + error)
      alert("Ошибка при запросе данных от БД.");
    }
  );

var dTypeListTMP = [], dViewListTMP = []

$(function() {

  //select filter DTYPE
  $('select[name="dtype"]').on('change', function() {
    //alert( this.value );
    var eVal = parseInt(this.value, 10);
    var eValTxT = $('select[name="dtype"] option:selected').text();
    if (eVal > 0)
    {
      dViewListTMP = []
      $.each(dViewList, function (i, item) {
        //if (parseInt(i, 10) === eVal) dTypeListTMP[i.toString()] = item;
        //select codes on dTypeList & views
        //if (contains(dTypeList, eValTxT)) dTypeListTMP[i.toString()] = item;
        //if (typeof a === "undefined") return false;
        for (var b = 0; b < dTypeList.length; b++) {
            if (dTypeList[b] === eValTxT) {
                //true
                dViewListTMP[b] = dViewList[b];
            }
        }
        
      });
      fillDName(dViewListTMP)
      //$('select[name="dname"]').val(eVal);
    }
    else
    {
      //fillDType(dTypeList)
      fillDName(dViewList)
    }
  });
  
  //select filter DNAME
  /*$('select[name="dname"]').on('change', function() {
    //alert( this.value );
    var eVal = parseInt(this.value, 10);
    if (eVal > 0)
    {
      dTypeListTMP = []
      $.each(dTypeList, function (i, item) {
        if (parseInt(i, 10) === eVal) dTypeListTMP[i.toString()] = item;
      });
      fillDType(dTypeListTMP)
      $('select[name="dtype"]').val(eVal);
    }
    else
    {
      fillDType(dTypeList)
      fillDName(dViewList)
    }
  });*/
  
  // "select[name="dobj"]" trigger - show or hide submit button
  $('select[name="dobj"]').on('change', function() {
    //alert( this.value );
    var eVal = this.value.toString();
    if (eVal.length > 0)
    {
      $('.formdiv.subm').show()
    }
    else
    {
      $('.formdiv.subm').hide()
    }
  });
  
  //$('form.lb-form input[name="ddatestart"], form.lb-form input[name="ddateend"]').on('change', function() {
    //$('form.lb-form input[name="dnum"]').change();
  //});
  //on enter trigger
  $(document).on("keypress",'form.lb-form input[name="ddatestart"], form.lb-form input[name="ddateend"]', function(e){
    if (e.keyCode == 13) { $('form.lb-form input[name="dnum"]').change(); }
  });

  
});

//on write NUMBER //keypress change paste
$(document).on("change",'form.lb-form input[name="dnum"]', function(e){
  
    if (parseInt($('form.lb-form select[name="dname"]').val(), 10) < 1)
    {
      alert('Не выбран "Вид данных"');
      return false;
    }
  
    //get event objs
    let dataEO
    dataEO = api_call('log_get_event_objs', {"metadata_id" : $('form.lb-form select[name="dname"]').val(), "data_string" : $('form.lb-form input[name="dnum"]').val(), "date_start" : $('form.lb-form input[name="ddatestart"]').val(), "date_end" : $('form.lb-form input[name="ddateend"]').val()}, true, true)
  
    dataEO
    .then(
      result => {
        //alert("OK (log_get_event_objs)");
        //dump(result)
        dObjList = [] //clear
        //parse objects
        /*for (var i in result) {
          var elRef = result[i]['ref_ones'].toString()
          var elStr = result[i]['data_structure'].toString()
          var elN = result[i]['data_string'].toString()
          dObjList[] = {"data_string":elN, "data_structure":elStr, "ref_ones":elRef}
        }*/
        dObjList = result
        // update objects list & show objs list
        fillDObj(dObjList)
      
        $(".formdiv.obj").fadeIn();
      
        //show submit button on "select[name="dobj"]" trigger
      },
      error => {
        //dump("Rejected: " + error)
        alert("Не удалось получить данные (log_get_event_objs).");
      }
    );
  
  return false;
});
//on enter trigger
$(document).on("keypress",'form.lb-form input[name="dnum"]', function(e){
  if (e.keyCode == 13) { $('form.lb-form input[name="dnum"]').change(); }
});

function fillMTable(data) 
{
  var r = new Array(), j = -1;
  for (var key=0, size=data.length; key<size; key++)
  {
     r[++j] ='<tr><td class="one">';
     r[++j] = data[key]["date_time"].replace("T"," "); //Дата изменения
     r[++j] = '</td><td class="two">';
     r[++j] = data[key]["user_name"]; //User
     r[++j] = '</td><td class="three">';
     r[++j] = data[key]["computer_name"]; //Компьютер
     r[++j] = '</td><td class="four">';
     r[++j] = data[key]["data_string"]; //Пользователь
     r[++j] = '</td><td class="five">';
     r[++j] = data[key]["data_structure"]; //Data Structure
     r[++j] = '</td><td class="six">';
     r[++j] = data[key]["comment"]; //Комментарий
     r[++j] = '</td><td class="sev">';
     r[++j] = data[key]["event_type_name"]; //Событие
     r[++j] = '</td><td class="ei">';
     r[++j] = data[key]["application_name"]; //Приложение
     r[++j] = '</td></tr>';
  }
  $('table.m-table tbody').html(r.join(''));
}

//on submit form
$(document).on("click","button.docsubmit", function(){
  let objVal = $('select#dobjSel').val();
  
  if (!objVal || objVal.length < 2) return false;
  
  //get result list
  let dataGR
  dataGR = api_call('log_get_results', {"ref_ones" : objVal, "date_start" : $('form.lb-form input[name="ddatestart"]').val(), "date_end" : $('form.lb-form input[name="ddateend"]').val()}, true, true)
  
  dataGR
  .then(
    result => {
      //alert("OK (log_get_results)");
      //dump(result)
      
      //set current obj name
      let startDate = $('form.lb-form input[name="ddatestart"]').val()
      let endDate = $('form.lb-form input[name="ddateend"]').val()
      let objFDate = ""
      if (startDate.length > 5) { objFDate += "с " + startDate }
      if (objFDate.length > 0 && endDate.length > 5) { objFDate += " " }
      if (endDate.length > 5) { objFDate += "до " + endDate }
      if (objFDate.length > 0) { objFDate = " (" + objFDate + ")" }
      $(".ib-obj-name").html($('select#dobjSel option:selected').text() + objFDate);
      
      //fill table
      fillMTable(result)
    },
    error => {
      //dump("Rejected: " + error)
      alert("Не удалось получить данные (log_get_results).");
    }
  );
  
  return false;
});



// TEST
/*$(function() {
var myArray = [
{"date_time":"1", "user_name":"11", "computer_name":"111", "data_string":"1111", "data_structure":"11111", "comment":"111111"},
{"date_time":"2", "user_name":"22", "computer_name":"222", "data_string":"2222", "data_structure":"22222", "comment":"222222"},
{"date_time":"3", "user_name":"33", "computer_name":"333", "data_string":"3333", "data_structure":"33333", "comment":"333333"},
]
//fillMTable(myArray);

});*/